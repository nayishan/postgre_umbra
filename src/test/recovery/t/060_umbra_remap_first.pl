# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the first non-identity mapping in the no-superblock MAP layout.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant INVALID_BLOCK_NUMBER => 0xFFFFFFFF;
use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_GROUP_TOTAL_PAGES => 8194;

sub file_blocks
{
	my ($path, $block_size) = @_;
	my $size = -s $path;

	defined($size) or BAIL_OUT("could not stat \"$path\"");
	$size % $block_size == 0
	  or BAIL_OUT("size of \"$path\" is not block-aligned");
	return $size / $block_size;
}

sub main_map_entry_offset
{
	my ($block_size, $lblkno) = @_;
	my $entries_per_page = int($block_size / 4);
	my $fork_page = int($lblkno / $entries_per_page);
	my $group = int($fork_page / MAP_GROUP_MAIN_PAGES);
	my $map_block = $group * MAP_GROUP_TOTAL_PAGES + 2
	  + ($fork_page % MAP_GROUP_MAIN_PAGES);

	return $map_block * $block_size
	  + ($lblkno % $entries_per_page) * 4;
}

sub read_main_map_entry
{
	my ($path, $block_size, $lblkno) = @_;
	my $offset = main_map_entry_offset($block_size, $lblkno);
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

	# BlockNumber is stored as a native-endian uint32.
	return unpack('L', $buffer);
}

sub visible_heap_blocks
{
	my ($node) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		q{SELECT COALESCE(max((ctid::text::point)[0]::integer) + 1, 0)
		  FROM umbra_remap_first;});
}

my $node = PostgreSQL::Test::Cluster->new('remap_first');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
fsync = on
full_page_writes = on
max_wal_senders = 0
recovery_prefetch = on
wal_level = minimal
});
$node->start;

my $block_size = 0 + $node->safe_psql(
	'postgres',
	q{SELECT pg_size_bytes(current_setting('block_size'));});
my $entries_per_page = int($block_size / 4);
my $row_count = $entries_per_page + 32;
my $payload_repeats = int($block_size / 64);

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_remap_first(id integer, payload text)
  WITH (autovacuum_enabled = false, vacuum_truncate = true);
ALTER TABLE umbra_remap_first ALTER COLUMN payload SET STORAGE PLAIN;
});
$node->safe_psql(
	'postgres', qq{
INSERT INTO umbra_remap_first
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, $row_count) AS g;
CHECKPOINT;
});

my $relpath = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_remap_first');});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
# pg_relation_size() reports the retained physical fork size in Umbra.  This
# relation stores one visible tuple per page, so its highest ctid observes the
# logical range exercised by this test instead.
my $logical_before = visible_heap_blocks($node);
my $physical_before = file_blocks($data_path, $block_size);

cmp_ok($logical_before, '>', $entries_per_page,
	'identity relation crosses a MAIN MAP page boundary');
cmp_ok($physical_before, '>=', $logical_before,
	'identity phase materializes every logical block');
is(read_main_map_entry($map_path, $block_size, 0), 0,
	'identity phase maps logical block 0 to physical block 0');
is(
	read_main_map_entry($map_path, $block_size, $entries_per_page),
	$entries_per_page,
	'identity phase maps the first entry on the second MAP page');

# VACUUM truncates the same relfilenode.  SQL TRUNCATE cannot be used here:
# it replaces the relation storage and therefore cannot produce L != P.
$node->safe_psql('postgres', 'DELETE FROM umbra_remap_first');
$node->safe_psql(
	'postgres',
	'VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umbra_remap_first');

is(
	$node->safe_psql(
		'postgres',
		q{SELECT pg_relation_filepath('umbra_remap_first');}),
	$relpath,
	'VACUUM truncate keeps the same relation storage');
is(visible_heap_blocks($node), 0,
	'VACUUM truncate resets the logical EOF');
is(file_blocks($data_path, $block_size), $physical_before,
	'logical truncate retains the append-only physical EOF');

$node->safe_psql('postgres', 'CHECKPOINT');
is(
	read_main_map_entry($map_path, $block_size, 0),
	INVALID_BLOCK_NUMBER,
	'truncate clears the first MAIN mapping');
is(
	read_main_map_entry(
		$map_path, $block_size, $entries_per_page - 1),
	INVALID_BLOCK_NUMBER,
	'truncate clears the end of the first MAIN MAP page');
is(
	read_main_map_entry($map_path, $block_size, $entries_per_page),
	INVALID_BLOCK_NUMBER,
	'truncate clears the start of the second MAIN MAP page');
is(
	read_main_map_entry($map_path, $block_size, $logical_before - 1),
	INVALID_BLOCK_NUMBER,
	'truncate clears the old logical tail');

my $mapping_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres',
	q{INSERT INTO umbra_remap_first VALUES (99, 'before-fpi');});
my $mapping_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

my $logical_after = visible_heap_blocks($node);
my $physical_after = file_blocks($data_path, $block_size);
my $mapping_details =
  "first_lblkno 0 first_pblkno $physical_before nblocks $logical_after";

cmp_ok($logical_after, '>', 0, 're-extension advances the logical EOF');
cmp_ok($physical_after, '>', $physical_before,
	're-extension appends beyond the retained physical EOF');
command_like(
	[
		'pg_waldump', '--bkp-details',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $mapping_wal_start,
		'--end' => $mapping_wal_end,
	],
	qr/fork main blk 0[^\n]*; remap: \Q$mapping_details\E/,
	'pg_waldump decodes the exact first-born mapping range');

# The first post-truncate data WAL record owns the new mapping.  An immediate
# stop leaves recovery to install that exact mapping before replaying data WAL.
$node->stop('immediate');
$node->start;

is(
	$node->safe_psql(
		'postgres',
		q{SELECT id::text || '|' || payload FROM umbra_remap_first;}),
	'99|before-fpi',
	'crash recovery reads data through the new mapping');
is(visible_heap_blocks($node), $logical_after,
	'crash recovery restores the logical EOF');
is(file_blocks($data_path, $block_size), $physical_after,
	'crash recovery preserves the exact physical EOF');

$node->safe_psql('postgres', 'CHECKPOINT');
is(
	read_main_map_entry($map_path, $block_size, 0),
	$physical_before,
	'logical block 0 maps to the first block after the old physical EOF');
isnt(read_main_map_entry($map_path, $block_size, 0), 0,
	're-extension produces an L != P mapping');

# After this checkpoint, the first update of the existing page carries a
# PostgreSQL full-page image.  Replay must still translate L through the MAP.
my $updated_block = $node->safe_psql(
	'postgres', q{
UPDATE umbra_remap_first SET payload = 'after-fpi' WHERE id = 99
RETURNING (ctid::text::point)[0]::integer;
});
is($updated_block, '0', 'FPI update stays on logical block 0');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres',
		q{SELECT id::text || '|' || payload FROM umbra_remap_first;}),
	'99|after-fpi',
	'FPI redo updates the existing non-identity page');
is(
	read_main_map_entry($map_path, $block_size, 0),
	$physical_before,
	'FPI redo does not replace the first-born mapping');

# New permanent relations can skip incremental WAL under wal_level=minimal.
# Force both end-of-xact durability choices and verify them with a crash: the
# high threshold emits newpage WAL, while zero forces immediate synchronization.
my $small_rows = 64;
my $sync_rows = 4096;
$node->safe_psql(
	'postgres', qq{
SET wal_skip_threshold = '1GB';
BEGIN;
CREATE TABLE umbra_skip_small(id integer);
INSERT INTO umbra_skip_small SELECT g FROM generate_series(1, $small_rows) g;
COMMIT;
});
$node->safe_psql(
	'postgres', qq{
SET wal_skip_threshold = 0;
BEGIN;
CREATE TABLE umbra_skip_sync(id integer);
INSERT INTO umbra_skip_sync SELECT g FROM generate_series(1, $sync_rows) g;
COMMIT;
});

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*)::text || '|' || sum(id) FROM umbra_skip_small;}),
	$small_rows . '|' . ($small_rows * ($small_rows + 1) / 2),
	'skip-WAL newpage path survives an immediate crash');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*)::text || '|' || sum(id) FROM umbra_skip_sync;}),
	$sync_rows . '|' . ($sync_rows * ($sync_rows + 1) / 2),
	'skip-WAL immediate-sync path survives an immediate crash');

# SP-GiST can extend an inner page, reject it because its block-number parity
# is unsuitable, and never reference that zero page in WAL.  The next
# first-born WAL record must own the complete contiguous mapping gap.
$node->safe_psql(
	'postgres', 'CREATE TABLE umbra_remap_spgist(ir int4range)');
$node->safe_psql(
	'postgres',
	'CREATE INDEX umbra_remap_spgist_idx ON umbra_remap_spgist USING spgist (ir)');
$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_remap_spgist
SELECT int4range(g, g + 10) FROM generate_series(1, 2000) AS g;
});

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM umbra_remap_spgist;}),
	'2000',
	'SP-GiST first-born gap survives an immediate crash');
is(
	$node->safe_psql(
		'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM umbra_remap_spgist WHERE ir @> 1000;
}),
	'10',
	'SP-GiST index reads through the recovered mapping');

$node->stop;
done_testing();
