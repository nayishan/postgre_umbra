# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify repeated redo when an earlier attempt already replayed relation DROP.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $primary = PostgreSQL::Test::Cluster->new('redo_drop_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = on
max_wal_size = '4GB'
});
$primary->start;

$primary->safe_psql(
	'postgres', q{
CREATE TABLE umbra_redo_drop_old(id integer, payload text)
  WITH (fillfactor = 50, autovacuum_enabled = false);
ALTER TABLE umbra_redo_drop_old ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_redo_drop_old
SELECT g, repeat('x', 400) FROM generate_series(1, 4000) AS g;
CREATE TABLE umbra_redo_drop_survivor(marker integer);
INSERT INTO umbra_redo_drop_survivor VALUES (1);
CHECKPOINT;
});

my $old_path = $primary->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_redo_drop_old'::regclass);});
my $old_filenode = $primary->safe_psql(
	'postgres',
	q{SELECT pg_relation_filenode('umbra_redo_drop_old'::regclass);});

$primary->backup('redo_drop_backup');
my $standby = PostgreSQL::Test::Cluster->new('redo_drop_standby');
$standby->init_from_backup(
	$primary, 'redo_drop_backup', has_streaming => 1);
$standby->start;

# Read before checkpoint so hint-bit WAL cannot consume the target page's
# first post-checkpoint full-page image before the UPDATE below.
my $target_block = 0 + $primary->safe_psql(
	'postgres', q{
SELECT (ctid::text::point)[0]::integer
FROM umbra_redo_drop_old WHERE id = 2000;
});
my $last_block = 0 + $primary->safe_psql(
	'postgres', q{
SELECT max((ctid::text::point)[0]::integer) FROM umbra_redo_drop_old;
});
cmp_ok($target_block, '>', 0, 'target existing page is above block zero');
cmp_ok($target_block, '<', $last_block,
	'target existing page leaves a recovery scratch gap before EOF');

$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres', 'CHECKPOINT');
my $restart_redo = $standby->safe_psql(
	'postgres', q{SELECT redo_lsn FROM pg_control_checkpoint();});

my $wal_start = $primary->safe_psql(
	'postgres', q{SELECT pg_current_wal_insert_lsn();});
$primary->safe_psql(
	'postgres', q{
UPDATE umbra_redo_drop_old SET payload = repeat('y', 400) WHERE id = 2000;
});
my $wal_end = $primary->safe_psql(
	'postgres', q{SELECT pg_current_wal_insert_lsn();});
is(
	0 + $primary->safe_psql(
		'postgres', q{
SELECT (ctid::text::point)[0]::integer
FROM umbra_redo_drop_old WHERE id = 2000;
}),
	$target_block,
	'existing-page update remains on the target logical block');

$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
my ($wal_dump, $wal_dump_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $primary->data_dir . '/pg_wal',
		'--start', $wal_start, '--end', $wal_end
	]);
is($wal_dump_stderr, '', 'pg_waldump reads existing-page update WAL');
my @target_records = grep {
	/rel \d+\/\d+\/$old_filenode fork main blk $target_block/
} split(/\n/, $wal_dump);
ok(grep(/\bFPW\b/, @target_records),
	'existing-page update carries a full-page image');
ok(!grep(/; remap:/, @target_records),
	'existing-page full-page image has no first-born remap header');

$primary->safe_psql(
	'postgres', q{
INSERT INTO umbra_redo_drop_old
SELECT g, repeat('z', 400) FROM generate_series(4001, 5000) AS g;
});

$primary->safe_psql(
	'postgres', q{
DROP TABLE umbra_redo_drop_old;
INSERT INTO umbra_redo_drop_survivor VALUES (2);
});
$primary->wait_for_replay_catchup($standby);

is(
	$standby->safe_psql(
		'postgres', q{SELECT to_regclass('umbra_redo_drop_old') IS NULL;}),
	't',
	'standby replays the relation DROP');
is(
	$standby->safe_psql(
		'postgres',
		"SELECT redo_lsn = '$restart_redo'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't',
	'standby redo point still precedes the existing-page image');
ok(!-e $standby->data_dir . "/${old_path}_map",
	'first replay removes the old relation MAP');

$standby->stop('immediate');
my $standby_restarted = $standby->start(fail_ok => 1);
ok($standby_restarted,
	'second recovery rebuilds a disposable mapping for the old full-page image');
BAIL_OUT('standby failed during the second replay')
  unless $standby_restarted;
$primary->wait_for_replay_catchup($standby);

is(
	$standby->safe_psql('postgres', q{SELECT pg_is_in_recovery();}),
	't',
	'standby remains in recovery after the second replay');
is(
	$standby->safe_psql(
		'postgres', q{
SELECT array_agg(marker ORDER BY marker) FROM umbra_redo_drop_survivor;
}),
	'{1,2}',
	'second recovery reaches WAL after the DROP');
ok(!-e $standby->data_dir . "/${old_path}_map",
	'second replay removes the disposable MAP at DROP');

$primary->stop('immediate');
$standby->stop('immediate');
done_testing();
