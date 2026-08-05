# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify MAIN-fork hint delta WAL, crash redo, and the auxiliary-FSM FPI path.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $page_index = int($logical_block / $entries_per_page);
	my $entry_index = $logical_block % $entries_per_page;
	my $group = int($page_index / 256);
	my $map_block = 1 + $group * 258 + 2 + ($page_index % 256);
	my $offset = $map_block * $block_size + int($entry_index / 4);
	my $byte;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $byte, 1);
	defined($nread) && $nread == 1
	  or BAIL_OUT("could not read selector from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return (unpack('C', $byte) >> (($entry_index % 4) * 2)) & 0x03;
}

sub xmin_is_committed
{
	my ($node, $relation) = @_;

	return $node->safe_psql(
		'postgres', qq[
SELECT EXISTS (
  SELECT 1
  FROM heap_page_items(get_raw_page('$relation', 0)) AS item
  CROSS JOIN LATERAL
    heap_tuple_infomask_flags(item.t_infomask, item.t_infomask2) AS flags
  WHERE item.lp = 1
    AND 'HEAP_XMIN_COMMITTED' = ANY (flags.raw_flags)
);]);
}

sub dump_wal
{
	my ($node, $start_lsn, $end_lsn, $label) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	is(normalize_waldump_stderr($stderr), '', "$label: pg_waldump reads WAL");
	return split(/(?=^rmgr: )/m, $dump);
}

my $node = PostgreSQL::Test::Cluster->new('umbra_hint_delta');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
mapwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
wal_log_hints = on
max_wal_size = '4GB'
]);
$node->start;

plan skip_all => 'extension pageinspect not installed'
  unless $node->check_extension('pageinspect');
$node->safe_psql('postgres', 'CREATE EXTENSION pageinspect');

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hint_delta(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_hint_delta VALUES (1, 'before');
CHECKPOINT;
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_hint_delta'::regclass);]);
my $filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
# A single-row table starts on block zero; do not scan it before the hint test.
my $target_block = 0;

is(xmin_is_committed($node, 'umbra_hint_delta'), 'f',
	'checkpointed tuple has no committed hint bit');
my $initial_slot = read_active_slot($map_path, $block_size, $target_block);
is($initial_slot, 0, 'checkpointed tuple starts at slot zero');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umbra_hint_delta WHERE id = 1;]),
	'before', 'visibility check reads the tuple');
my $updated_block = 0 + $node->safe_psql(
	'postgres', q[
UPDATE umbra_hint_delta SET payload = 'after' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]);
is($updated_block, $target_block,
	'ordinary update remains on the hint delta logical block');
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
my @hint_wal = dump_wal($node, $wal_start, $wal_end, 'hint delta');

my @hint_records = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
} @hint_wal;
is(scalar(@hint_records), 1,
	'first MAIN hint emits one HINT_DELTA record');
my $hint_record = $hint_records[0] // '';
like($hint_record, qr/desc: HINT_DELTA fragments 1, final bytes [12]\b/,
	'HINT_DELTA stores only the final changed hint bytes');
unlike($hint_record, qr/\bFPW\b/,
	'HINT_DELTA does not carry a full-page image');
my ($source_slot, $target_slot) =
	$hint_record =~ /slot shift: source_slot ([0-2]) target_slot ([0-2])\b/;
ok(defined($source_slot) && defined($target_slot),
	'HINT_DELTA records a source and target slot');
is($source_slot, $initial_slot,
	'HINT_DELTA names the checkpointed selector as its source');
is($target_slot, 1,
	'HINT_DELTA advances the selector from slot zero to slot one');

my @main_hint_fpi = grep {
	/desc: FPI_FOR_HINT/ &&
	/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
} @hint_wal;
is(scalar(@main_hint_fpi), 0,
	'first MAIN hint does not emit FPI_FOR_HINT');
my @ordinary_updates = grep {
	/rmgr: Heap/ && /desc: (?:HOT_)?UPDATE/ &&
	/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
} @hint_wal;
cmp_ok(scalar(@ordinary_updates), '>=', 1,
	'ordinary update follows HINT_DELTA on the same logical page');
ok(!grep(/\bFPW\b/, @ordinary_updates),
	'following ordinary update does not need a full-page image');
ok(!grep(/slot shift:/, @ordinary_updates),
	'following ordinary update reuses the HINT_DELTA mapping');

# mapwriter is disabled, so the selector on disk remains the old baseline
# until crash redo publishes the durable HINT_DELTA target.
is(read_active_slot($map_path, $block_size, $target_block), $source_slot,
	'selector target is not checkpointed before the immediate stop');
$node->stop('immediate');
$node->start;
is(xmin_is_committed($node, 'umbra_hint_delta'), 't',
	'crash redo restores the committed hint bit');
is(read_active_slot($map_path, $block_size, $target_block), $target_slot,
	'crash redo publishes the HINT_DELTA selector target');
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umbra_hint_delta WHERE id = 1;]),
	'after', 'ordinary WAL follows the recovered hint delta');

# A later TRUNCATE removes the old relfilenode's _map root before restart
# redo reaches its HINT_DELTA.  The hint record must use normal missing-page
# accounting instead of trying to publish a selector without a root.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hint_delta_replaced(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_hint_delta_replaced VALUES (1, 'old storage');
CHECKPOINT;
]);
my $replaced_relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_hint_delta_replaced'::regclass);]);
my $replaced_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta_replaced'::regclass);]);
my $replaced_map_path = $node->data_dir . "/${replaced_relpath}_map";
ok(-e $replaced_map_path, 'old storage has a mapping root before its hint');
is(xmin_is_committed($node, 'umbra_hint_delta_replaced'), 'f',
	'old storage has no committed hint bit before replacement coverage');

my $replaced_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is($node->safe_psql(
		'postgres', q[
SELECT payload FROM umbra_hint_delta_replaced WHERE id = 1;]),
	'old storage', 'visibility check creates an old-storage hint delta');
$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
my $replaced_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
my @replaced_wal = dump_wal(
	$node, $replaced_wal_start, $replaced_wal_end, 'old storage hint delta');
my @replaced_hints = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$replaced_filenode fork main blk 0\b/
} @replaced_wal;
is(scalar(@replaced_hints), 1,
	'old storage emits one HINT_DELTA before replacement');
unlike($replaced_hints[0] // '', qr/\bFPW\b/,
	'old-storage HINT_DELTA has no full-page image');

$node->safe_psql('postgres', 'TRUNCATE umbra_hint_delta_replaced;');
my $replacement_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta_replaced'::regclass);]);
isnt($replacement_filenode, $replaced_filenode,
	'TRUNCATE replaces the old storage');
ok(!-e $replaced_map_path,
	'TRUNCATE removes the old storage mapping root before crash recovery');

$node->stop('immediate');
is($node->start(fail_ok => 1), 1,
	'crash redo clears the old-storage dependency through TRUNCATE');
is($node->safe_psql(
		'postgres', q[SELECT count(*) FROM umbra_hint_delta_replaced;]),
	'0', 'replacement storage remains empty after crash redo');
$node->safe_psql(
	'postgres', q[INSERT INTO umbra_hint_delta_replaced VALUES (2, 'replacement storage');]);
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umbra_hint_delta_replaced WHERE id = 2;]),
	'replacement storage', 'replacement storage remains writable after crash redo');

# HINT_DELTA remains image-free even when xlog2 consistency checking is
# enabled.  Its source/target transition and final-byte ranges are the complete
# recovery contract.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hint_delta_consistency(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_hint_delta_consistency VALUES (1, 'consistency');
CHECKPOINT;
]);
my $consistency_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta_consistency'::regclass);]);
is(xmin_is_committed($node, 'umbra_hint_delta_consistency'), 'f',
	'xlog2 consistency tuple has no committed hint bit before the read');
my $consistency_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is($node->safe_psql(
		'postgres', q[
SET wal_consistency_checking = 'xlog2';
SELECT payload FROM umbra_hint_delta_consistency WHERE id = 1;
]), 'consistency', 'xlog2 consistency checking permits a MAIN hint');
# Hint-only WAL need not be flushed by its read-only query.
$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
my $consistency_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
my @consistency_wal = dump_wal(
	$node, $consistency_wal_start, $consistency_wal_end, 'xlog2 consistency');
my @consistency_hints = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$consistency_filenode fork main blk 0\b/ &&
	/slot shift: source_slot 0 target_slot 1\b/
} @consistency_wal;
is(scalar(@consistency_hints), 1,
	'xlog2 consistency checking records one HINT_DELTA');
unlike($consistency_hints[0] // '', qr/\bFPW\b/,
	'xlog2 consistency checking does not add a full-page image');
$node->stop('immediate');
$node->start;
is(xmin_is_committed($node, 'umbra_hint_delta_consistency'), 't',
	'xlog2 consistency HINT_DELTA survives crash redo');
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umbra_hint_delta_consistency WHERE id = 1;]),
	'consistency', 'consistency-checked HINT_DELTA retains tuple data');

# FSM uses MarkBufferDirtyHint() directly and retains FPI_FOR_HINT while using
# that image to rotate its own selector.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hint_delta_fsm(id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_hint_delta_fsm
SELECT g, repeat('x', 1200) FROM generate_series(1, 1000) AS g;
VACUUM (ANALYZE) umbra_hint_delta_fsm;
]);
my $fsm_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta_fsm'::regclass);]);
cmp_ok($node->safe_psql(
		'postgres', q[SELECT pg_relation_size('umbra_hint_delta_fsm', 'fsm');]),
	'>', 0, 'FSM fork exists before FPI-path coverage');
$node->safe_psql(
	'postgres', q[
DELETE FROM umbra_hint_delta_fsm;
CHECKPOINT;
]);

my $fsm_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q[
VACUUM (DISABLE_PAGE_SKIPPING, TRUNCATE FALSE) umbra_hint_delta_fsm;
]);
# FSM hint WAL is not tied to a transaction commit, so force it to disk before
# asking pg_waldump for the captured range.
$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
my $fsm_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
my @fsm_wal = dump_wal($node, $fsm_wal_start, $fsm_wal_end, 'FSM FPI path');
my @fsm_hint_fpi = grep {
	/desc: FPI_FOR_HINT/ &&
	/rel \d+\/\d+\/$fsm_filenode fork fsm blk \d+\b/
} @fsm_wal;
cmp_ok(scalar(@fsm_hint_fpi), '>=', 1,
	'FSM maintenance retains at least one FPI_FOR_HINT record');
ok(grep(/\bFPW\b/, @fsm_hint_fpi),
	'FSM FPI_FOR_HINT retains a full-page image');
my @fsm_hint_delta = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$fsm_filenode fork fsm blk \d+\b/
} @fsm_wal;
is(scalar(@fsm_hint_delta), 0,
	'FSM maintenance never emits HINT_DELTA');
ok(grep(/slot shift: source_slot [0-2] target_slot [0-2]/, @fsm_hint_fpi),
	'FSM FPI_FOR_HINT carries its active-slot transition');

# A missing root without later lifecycle WAL must remain an invalid-page
# dependency; otherwise a HINT_DELTA source baseline could be lost silently.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_hint_delta_unresolved(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_hint_delta_unresolved VALUES (1, 'unresolved');
CHECKPOINT;
]);
my $unresolved_relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_hint_delta_unresolved'::regclass);]);
my $unresolved_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_hint_delta_unresolved'::regclass);]);
my $unresolved_main_path = $node->data_dir . "/$unresolved_relpath";
my $unresolved_map_path = "${unresolved_main_path}_map";
is(xmin_is_committed($node, 'umbra_hint_delta_unresolved'), 'f',
	'unresolved storage has no committed hint bit before its hint WAL');

my $unresolved_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is($node->safe_psql(
		'postgres', q[
SELECT payload FROM umbra_hint_delta_unresolved WHERE id = 1;]),
	'unresolved', 'visibility check creates an unresolved hint delta');
$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
my $unresolved_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
my @unresolved_wal = dump_wal(
	$node, $unresolved_wal_start, $unresolved_wal_end,
	'unresolved storage hint delta');
my @unresolved_hints = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$unresolved_filenode fork main blk 0\b/
} @unresolved_wal;
is(scalar(@unresolved_hints), 1,
	'unresolved storage emits one HINT_DELTA');
unlike($unresolved_hints[0] // '', qr/\bFPW\b/,
	'unresolved HINT_DELTA has no full-page image');

$node->stop('immediate');
unlink $unresolved_map_path
  or BAIL_OUT("could not remove \"$unresolved_map_path\": $!");
truncate($unresolved_main_path, 0)
  or BAIL_OUT("could not truncate \"$unresolved_main_path\": $!");
is($node->start(fail_ok => 1), 0,
	'crash redo rejects an unresolved hint delta baseline');
like(slurp_file($node->logfile), qr/WAL contains references to invalid pages/,
	'unresolved hint delta is reported through invalid-page accounting');

done_testing();
