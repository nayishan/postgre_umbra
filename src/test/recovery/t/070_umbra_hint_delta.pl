# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that the first hint update after a checkpoint uses an exact byte
# delta and old-P/new-P remap instead of a full-page image.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_GROUP_TOTAL_PAGES => 8194;

sub read_main_map_entry
{
	my ($path, $block_size, $lblkno) = @_;
	my $entries_per_page = int($block_size / 4);
	my $fork_page = int($lblkno / $entries_per_page);
	my $group = int($fork_page / MAP_GROUP_MAIN_PAGES);
	my $map_block = 1 + $group * MAP_GROUP_TOTAL_PAGES + 2
	  + ($fork_page % MAP_GROUP_MAIN_PAGES);
	my $offset = $map_block * $block_size
	  + ($lblkno % $entries_per_page) * 4;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, 4);
	defined($nread) && $nread == 4
	  or BAIL_OUT("could not read a MAP entry from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return unpack('L', $buffer);
}

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

sub xmin_is_committed
{
	my ($node, $relation) = @_;

	return $node->safe_psql(
		'postgres', qq{
SELECT EXISTS (
  SELECT 1
  FROM heap_page_items(get_raw_page('$relation', 0)) AS item
  CROSS JOIN LATERAL
    heap_tuple_infomask_flags(item.t_infomask, item.t_infomask2) AS flags
  WHERE item.lp = 1
    AND 'HEAP_XMIN_COMMITTED' = ANY (flags.raw_flags)
);});
}

my $node = PostgreSQL::Test::Cluster->new('umbra_hint_delta');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
shared_buffers = '16MB'
map_compactor_enable = off
map_prealloc_main_low = 1073741823
wal_log_hints = on
});
$node->start;

plan skip_all => 'extension pageinspect not installed'
  unless $node->check_extension('pageinspect');
$node->safe_psql('postgres', 'CREATE EXTENSION pageinspect');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_hint_delta(id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_hint_delta VALUES (1, 'before');
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'))});
my $relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_hint_delta')});
my $filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_hint_delta')});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
my $physical_size_before = -s $data_path;
my $old_mapping = read_main_map_entry($map_path, $block_size, 0);

# This raw read does not perform tuple visibility checks.
is(xmin_is_committed($node, 'umbra_hint_delta'), 'f',
	'checkpointed tuple does not yet have the committed hint bit');

# Keep the target data buffer resident while more than seven full clock sweeps
# evict its ordinary MAP page from the 16-slot MAP pool.  The first visibility
# hint must reload that MAP page and wait for a real remap claim; a cache miss
# must not select FPI_FOR_HINT as an alternate WAL format.
$node->safe_psql(
	'postgres', q{
DO $$
BEGIN
  FOR i IN 1..128 LOOP
    EXECUTE format('CREATE TABLE umbra_hint_churn_%s(id integer)', i);
    EXECUTE format('INSERT INTO umbra_hint_churn_%s VALUES (1)', i);
  END LOOP;
END
$$;
});

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is(
	$node->safe_psql(
		'postgres', q{SELECT payload FROM umbra_hint_delta WHERE id = 1}),
	'before',
	'first visibility check reads the tuple');
is(xmin_is_committed($node, 'umbra_hint_delta'), 't',
	'first visibility check sets the committed hint bit in memory');

# The next ordinary WAL record must use the mapping published by HINT_DELTA,
# without taking another image or remapping the same logical page again.
my $updated_block = $node->safe_psql(
	'postgres', q{
UPDATE umbra_hint_delta SET payload = 'after' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;});
is($updated_block, '0', 'ordinary update remains on logical block zero');
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn()');

my ($wal, $wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $wal_start, '--end', $wal_end
	]);
is(normalize_waldump_stderr($wal_stderr), '',
	'pg_waldump reads the hint and ordinary update WAL chain');

my @wal_records = split(/(?=^rmgr: )/m, $wal);
my @hint_records = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
is(scalar(@hint_records), 1,
	'first committed hint emits one HINT_DELTA record');
my $hint_record = $hint_records[0] // '';
like($hint_record,
	qr/desc: HINT_DELTA fragments 1, final bytes [12]\b/,
	'HINT_DELTA carries only the changed heap hint bytes');
unlike($hint_record, qr/\bFPW\b/,
	'HINT_DELTA does not carry a full-page image');
like($hint_record,
	qr/; remap: lblkno 0 old_pblkno \d+ new_pblkno \d+/,
	'HINT_DELTA carries an exact old-P/new-P remap');

my ($wal_old_pblkno, $wal_new_pblkno) =
  $hint_record =~ /; remap: lblkno 0 old_pblkno (\d+) new_pblkno (\d+)/;
$wal_old_pblkno = -1 unless defined($wal_old_pblkno);
$wal_new_pblkno = -1 unless defined($wal_new_pblkno);
is($wal_old_pblkno, $old_mapping,
	'HINT_DELTA names the checkpointed physical baseline');
isnt($wal_new_pblkno, $wal_old_pblkno,
	'HINT_DELTA reserves a distinct physical target');

my @old_hint_fpi = grep {
	/desc: FPI_FOR_HINT/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
is(scalar(@old_hint_fpi), 0,
	'first committed hint does not emit the old FPI_FOR_HINT record');

my @ordinary_updates = grep {
	/rmgr: Heap/ && /desc: (?:HOT_)?UPDATE/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
cmp_ok(scalar(@ordinary_updates), '>=', 1,
	'ordinary update follows HINT_DELTA on the same logical page');
ok(!grep(/\bFPW\b/, @ordinary_updates),
	'ordinary update does not carry a full-page image');
ok(!grep(/; remap:/, @ordinary_updates),
	'ordinary update continues through the HINT_DELTA mapping');

is(-s $data_path, $physical_size_before,
	'remap target remains unwritten before the immediate crash');

$node->stop('immediate');
$node->start;

# Inspect raw tuple bytes before any ordinary relation scan can recreate the
# hint.  Recovery must first apply HINT_DELTA to old P and then replay UPDATE
# through the newly published mapping.
is(xmin_is_committed($node, 'umbra_hint_delta'), 't',
	'crash recovery restores the committed hint bit from the byte delta');
is(read_main_map_entry($map_path, $block_size, 0), $wal_new_pblkno,
	'crash recovery publishes the HINT_DELTA physical target');
cmp_ok(-s $data_path, '>=', ($wal_new_pblkno + 1) * $block_size,
	'crash recovery materializes the remap target');
is(
	$node->safe_psql(
		'postgres', q{SELECT payload FROM umbra_hint_delta WHERE id = 1}),
	'after',
	'ordinary WAL replays after the hint remap');

# A committed DROP can remove the old-P baseline before crash recovery reaches
# an earlier HINT_DELTA record.  Redo must defer that missing-page error until
# the later DROP record clears the normal invalid-page recovery dependency.
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_hint_drop(id integer)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_hint_drop VALUES (1);
CHECKPOINT;
});
my $drop_relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_hint_drop')});
my $drop_filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_hint_drop')});
my $drop_data_path = $node->data_dir . "/$drop_relpath";
my $drop_map_path = "${drop_data_path}_map";

is(xmin_is_committed($node, 'umbra_hint_drop'), 'f',
	'drop target starts without a committed hint bit');
my $drop_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is(
	$node->safe_psql(
		'postgres', q{SELECT id FROM umbra_hint_drop WHERE id = 1}),
	'1',
	'drop target visibility check emits a hint update');
is(xmin_is_committed($node, 'umbra_hint_drop'), 't',
	'drop target has the committed hint bit in memory');
$node->safe_psql('postgres', 'DROP TABLE umbra_hint_drop');
my $drop_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn()');

my ($drop_wal, $drop_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $drop_wal_start, '--end', $drop_wal_end
	]);
is(normalize_waldump_stderr($drop_wal_stderr), '',
	'pg_waldump reads the hint-before-drop WAL chain');
my @drop_hint_records = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$drop_filenode fork main blk 0/
} split(/(?=^rmgr: )/m, $drop_wal);
is(scalar(@drop_hint_records), 1,
	'dropped relation has one HINT_DELTA before its lifecycle WAL');

$node->stop('immediate');

# Normal DROP preserves checkpointed mapped files until the next checkpoint.
# Removing them here models a second recovery after the first attempt replayed
# DROP, unlinked the relation, and crashed before advancing the redo point.
unlink($drop_data_path)
  or BAIL_OUT("could not remove \"$drop_data_path\": $!");
unlink($drop_map_path)
  or BAIL_OUT("could not remove \"$drop_map_path\": $!");
$node->start;
is(
	$node->safe_psql(
		'postgres', q{SELECT to_regclass('umbra_hint_drop') IS NULL}),
	't',
	'DROP lifecycle WAL resolves the missing hint delta baseline');

# Without later lifecycle WAL, the same missing baseline must remain a recovery
# error.  Remove the relation files only after the hint WAL is durable so this
# final crash simulates an unresolved old-P dependency.
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_hint_missing(id integer)
  WITH (autovacuum_enabled = false);
CREATE TABLE umbra_hint_flush_marker(id integer);
INSERT INTO umbra_hint_missing VALUES (1);
CHECKPOINT;
});
my $missing_relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_hint_missing')});
my $missing_filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_hint_missing')});
my $missing_data_path = $node->data_dir . "/$missing_relpath";
my $missing_map_path = "${missing_data_path}_map";

is(xmin_is_committed($node, 'umbra_hint_missing'), 'f',
	'unresolved target starts without a committed hint bit');
my $missing_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is(
	$node->safe_psql(
		'postgres', q{SELECT id FROM umbra_hint_missing WHERE id = 1}),
	'1',
	'unresolved target visibility check emits a hint update');
$node->safe_psql(
	'postgres', 'INSERT INTO umbra_hint_flush_marker VALUES (1)');
my $missing_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn()');
my ($missing_wal, $missing_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $missing_wal_start, '--end', $missing_wal_end
	]);
is(normalize_waldump_stderr($missing_wal_stderr), '',
	'pg_waldump reads the unresolved hint WAL chain');
my @missing_hint_records = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$missing_filenode fork main blk 0/
} split(/(?=^rmgr: )/m, $missing_wal);
is(scalar(@missing_hint_records), 1,
	'unresolved relation has one durable HINT_DELTA record');

$node->stop('immediate');
unlink($missing_data_path)
  or BAIL_OUT("could not remove \"$missing_data_path\": $!");
unlink($missing_map_path)
  or BAIL_OUT("could not remove \"$missing_map_path\": $!");
ok(!$node->start(fail_ok => 1),
	'recovery rejects a hint delta with no baseline or lifecycle WAL');
ok($node->log_contains(qr/WAL contains references to invalid pages/),
	'unresolved hint delta is reported through invalid-page recovery');

done_testing();
