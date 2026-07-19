# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify repeated recovery across DROP and exact relation-locator reuse.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');
plan skip_all => 'SIGSTOP is not supported on Windows'
  if $PostgreSQL::Test::Utils::windows_os;

use constant MAP_GENERATION_LSN_OFFSET => 40;

my $stopped_checkpointer_pid;

END
{
	kill 'CONT', $stopped_checkpointer_pid
	  if defined $stopped_checkpointer_pid;
}

sub sql_literal
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub map_u64
{
	my ($node, $path, $offset) = @_;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($path) . ", $offset, 8, false), 'hex');");

	return unpack('Q', pack('H*', $hex));
}

my $primary = PostgreSQL::Test::Cluster->new('map_generation_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf',
	"allow_in_place_tablespaces = on\nautovacuum = off\n"
	  . "checkpoint_timeout = '1h'\nmax_wal_size = '4GB'\n"
	  . "restart_after_crash = on");
$primary->start;

my $regress_shlib = $ENV{REGRESS_SHLIB};
BAIL_OUT('REGRESS_SHLIB is not set') unless defined $regress_shlib;
$primary->safe_psql(
	'postgres',
	'CREATE FUNCTION regress_set_next_oid(oid) RETURNS oid AS '
	  . sql_literal($regress_shlib)
	  . ", 'regress_set_next_oid' LANGUAGE C STRICT;");

$primary->safe_psql(
	'postgres', q{
CREATE TABLESPACE umbra_generation_ts LOCATION '';
CREATE TABLE umbra_generation_old(id integer, payload name)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_generation_old SET TABLESPACE umbra_generation_ts;
INSERT INTO umbra_generation_old
SELECT g, md5(g::text)::name FROM generate_series(1, 20000) AS g;
DELETE FROM umbra_generation_old;
VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umbra_generation_old;
INSERT INTO umbra_generation_old
SELECT g, md5(g::text)::name FROM generate_series(20001, 24000) AS g;
CHECKPOINT;
});

my $old_oid = 0 + $primary->safe_psql(
	'postgres', q{SELECT 'umbra_generation_old'::regclass::oid;});
my $reused_filenode = 0 + $primary->safe_psql(
	'postgres',
	q{SELECT pg_relation_filenode('umbra_generation_old'::regclass);});
my $old_path = $primary->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_generation_old'::regclass);});
my $map_path = "${old_path}_map";
my $old_generation_lsn =
  map_u64($primary, $map_path, MAP_GENERATION_LSN_OFFSET);

isnt($reused_filenode, $old_oid,
	'old relation filenode is independent of its catalog OID');
isnt($old_generation_lsn, 0, 'old generation has a CREATE identity');

my ($old_overlap_id, $old_overlap_block) = split(/\|/, $primary->safe_psql(
		'postgres', q{
SELECT id, (ctid::text::point)[0]::integer
FROM umbra_generation_old
ORDER BY (ctid::text::point)[0]::integer, id
LIMIT 1;
}));
my ($old_high_id, $old_high_block) = split(/\|/, $primary->safe_psql(
		'postgres', q{
SELECT id, (ctid::text::point)[0]::integer
FROM umbra_generation_old
ORDER BY (ctid::text::point)[0]::integer DESC, id DESC
LIMIT 1;
}));
is($old_overlap_block, 0,
	'old generation overlaps the future generation at logical block zero');
cmp_ok($old_high_block, '>', 1,
	'old generation also has a page beyond the future generation EOF');

$primary->backup('map_generation_backup');
my $standby = PostgreSQL::Test::Cluster->new('map_generation_standby');
$standby->init_from_backup(
	$primary, 'map_generation_backup', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres', 'CHECKPOINT');
my $restart_redo_before = $standby->safe_psql(
	'postgres', q{SELECT redo_lsn FROM pg_control_checkpoint();});
my ($restart_redo_hi, $restart_redo_lo) = split('/', $restart_redo_before);
my $restart_redo_log = sprintf('%X/%08X', hex($restart_redo_hi),
	hex($restart_redo_lo));
$stopped_checkpointer_pid = $standby->safe_psql(
	'postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer';});
like($stopped_checkpointer_pid, qr/^[0-9]+$/,
	'have a standby checkpointer PID');
is(kill('STOP', $stopped_checkpointer_pid), 1,
	'standby checkpointer is stopped before DROP replay');

# These first post-checkpoint changes carry full-page images for the old
# generation.  The second recovery must discard them after locator reuse.
$primary->safe_psql(
	'postgres',
	"UPDATE umbra_generation_old SET payload = 'old-overlap-redo' "
	  . "WHERE id = $old_overlap_id; "
	  . "UPDATE umbra_generation_old SET payload = 'old-high-redo' "
	  . "WHERE id = $old_high_id;");
$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql(
		'postgres',
		"SELECT count(*) FROM umbra_generation_old WHERE "
		  . "(id = $old_overlap_id AND payload = 'old-overlap-redo') OR "
		  . "(id = $old_high_id AND payload = 'old-high-redo');"),
	'2', 'standby first replays both old-generation updates');

# On the second recovery this checkpoint is replayed after the future G2 root
# has forced an old-generation scratch reset, but before DROP or G2 CREATE.
# Its smgrdestroyall() must not forget that pre-create redo is disposable.
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_replay_catchup($standby);

$primary->safe_psql('postgres', 'DROP TABLE umbra_generation_old;');
my $old_main_file = $primary->data_dir . "/$old_path";
my $old_map_file = $primary->data_dir . "/$map_path";
ok(-e $old_main_file, 'normal DROP retains the MAIN filenode gate');
ok(-e $old_map_file, 'normal DROP retains the old MAP generation');

$primary->safe_psql('postgres', 'CHECKPOINT');
ok(!-e $old_main_file, 'checkpoint removes the old MAIN gate');
ok(!-e $old_map_file, 'checkpoint removes the old MAP generation');
$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql(
		'postgres',
		"SELECT redo_lsn = '$restart_redo_before'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'standby replayed DROP without advancing its local restartpoint');

$primary->safe_psql(
	'postgres', q{
CREATE TABLE umbra_generation_new(marker integer);
INSERT INTO umbra_generation_new VALUES (424242);
});
$primary->safe_psql(
	'postgres',
	"SELECT regress_set_next_oid($reused_filenode); "
	  . 'ALTER TABLE umbra_generation_new SET TABLESPACE umbra_generation_ts;');

is(0 + $primary->safe_psql(
		'postgres',
		q{SELECT pg_relation_filenode('umbra_generation_new'::regclass);}),
	$reused_filenode, 'new generation reuses the exact old filenode');
is($primary->safe_psql(
		'postgres',
		q{SELECT pg_relation_filepath('umbra_generation_new'::regclass);}),
	$old_path, 'new generation reuses the exact old relation locator');

$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql(
		'postgres', q{SELECT marker FROM umbra_generation_new;}),
	'424242', 'standby reads the reused generation before restartpoint');
my $new_generation_lsn =
  map_u64($standby, $map_path, MAP_GENERATION_LSN_OFFSET);
isnt($new_generation_lsn, 0, 'new generation has a CREATE identity');
isnt($new_generation_lsn, $old_generation_lsn,
	'new generation has a distinct CREATE identity');

# Killing the stopped checkpointer restarts recovery from before G1 DROP while
# the canonical files already belong to G2.
my $crash_log_offset = -s $standby->logfile;
is(PostgreSQL::Test::Utils::system_log(
		'pg_ctl', 'kill', 'KILL', $stopped_checkpointer_pid),
	0, 'standby checkpointer is killed before a newer restartpoint');
$stopped_checkpointer_pid = undef;
$standby->wait_for_log(
	qr/redo starts at \Q$restart_redo_log\E/, $crash_log_offset);
pass('second recovery starts before the old-generation DROP');
$standby->wait_for_log(
	qr/database system is ready to accept read-only connections/,
	$crash_log_offset);
ok($standby->poll_query_until('postgres', 'SELECT pg_is_in_recovery();'),
	'standby reconnects after second recovery');
$primary->wait_for_replay_catchup($standby);

is(map_u64($standby, $map_path, MAP_GENERATION_LSN_OFFSET),
	$new_generation_lsn,
	'second recovery restores the new generation identity');
is($standby->safe_psql(
		'postgres', q{SELECT marker FROM umbra_generation_new;}),
	'424242', 'new-generation data survives second recovery');

$standby->safe_psql('postgres', 'CHECKPOINT');
is($standby->safe_psql(
		'postgres',
		"SELECT redo_lsn > '$restart_redo_before'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'cleared invalid-page dependencies permit a newer restartpoint');

$primary->stop('immediate');
$standby->stop('immediate');
ok($standby->start(fail_ok => 1),
	'new generation survives restart from the newer restartpoint');
is($standby->safe_psql(
		'postgres', q{SELECT marker FROM umbra_generation_new;}),
	'424242', 'new-generation data remains readable after final restart');

$standby->stop;
done_testing();
