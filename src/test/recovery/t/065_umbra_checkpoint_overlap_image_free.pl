# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that an update shifted after checkpoint selection is image-free and
# crash redo materializes its captured source slot before publishing target.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $entry_index = $logical_block % $entries_per_page;
	my $map_block = 1 + int($logical_block / $entries_per_page);
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

sub source_slot_block
{
	my ($logical_block, $slot) = @_;
	my $chunk = int($logical_block / 32);

	return $chunk * (3 * 32) + $slot * 32 + $logical_block % 32;
}

sub read_physical_block
{
	my ($path, $block_size, $physical_block) = @_;
	my $offset = $physical_block * $block_size;
	my $page;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $page, $block_size);
	defined($nread) && $nread == $block_size
	  or BAIL_OUT("could not read block $physical_block from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return $page;
}

sub assert_image_free_shift_wal
{
	my ($node, $filenode, $target_block, $start_lsn, $end_lsn) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', 'pg_waldump reads the checkpoint-overlap update WAL');
	my @records = grep {
		/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
	} split(/\n/, $dump);
	is(scalar(@records), 1,
		'checkpoint-overlap update emits one target-block WAL record');
	unlike($records[0], qr/\bFPW\b/,
		'checkpoint-overlap shift is image-free');
	like($records[0],
		qr/slot shift: source_slot 1 target_slot 2 captured_source/,
		'checkpoint-overlap shift records its captured source');
}

my $node = PostgreSQL::Test::Cluster->new(
	'umbra_checkpoint_overlap_image_free');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
mapwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
max_wal_size = '4GB'
]);
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_checkpoint_overlap_image_free(
  id integer PRIMARY KEY,
  payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_checkpoint_overlap_image_free VALUES (1, 'initial');
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres', q[
SELECT pg_relation_filepath('umbra_checkpoint_overlap_image_free'::regclass);]);
my $filenode = $node->safe_psql(
	'postgres', q[
SELECT pg_relation_filenode('umbra_checkpoint_overlap_image_free'::regclass);]);
my $main_path = $node->data_dir . "/$relpath";
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_checkpoint_overlap_image_free WHERE id = 1;]);

$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'initial checkpoint persists slot 0');

my $first_block = 0 + $node->safe_psql(
	'postgres', q[
UPDATE umbra_checkpoint_overlap_image_free
SET payload = 'slot-one'
WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]);
is($first_block, $target_block, 'first update remains on one logical block');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach('umbra-checkpoint-after-map', 'wait')]);
my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-map');
pass('C1 flushes slot 1 selector before data-buffer writeback');

my $image_free_marker = 'p12-image-free-marker';
my $image_free_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
my $second_block = 0 + $node->safe_psql(
	'postgres', qq[
UPDATE umbra_checkpoint_overlap_image_free
SET payload = '$image_free_marker'
WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]);
my $image_free_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is($second_block, $target_block,
	'C1-overlap update remains on the selected logical block');
assert_image_free_shift_wal($node, $filenode, $target_block,
	$image_free_wal_start, $image_free_wal_end);

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-map')]);
$checkpoint->query_until(qr/checkpoint_done/, '');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-map')]);

is(read_active_slot($map_path, $block_size, $target_block), 1,
	'C1 leaves the durable selector at its captured source slot');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 1)), $image_free_marker) >= 0,
	'C1 writes the overlap page image to captured slot 1');

$node->stop('immediate');
$node->start;
is(read_active_slot($map_path, $block_size, $target_block), 2,
	'crash redo publishes the image-free target slot');
is($node->safe_psql(
		'postgres', q[
SELECT payload FROM umbra_checkpoint_overlap_image_free WHERE id = 1;]),
	$image_free_marker, 'crash redo restores the image-free update through slot 2');

$checkpoint->quit;
$node->stop;

done_testing();
