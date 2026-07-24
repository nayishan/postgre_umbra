# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a checkpoint starts its shift epoch before the fresh delay scan
# and records a later image-free publication before selecting buffers.

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
	is($stderr, '', 'pg_waldump reads the pre-selection update WAL');
	my @records = grep {
		/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
	} split(/\n/, $dump);
	is(scalar(@records), 1,
		'pre-selection update emits one target-block WAL record');
	unlike($records[0], qr/\bFPW\b/,
		'shift-epoch update omits its full-page image');
	like($records[0],
		qr/slot shift: source_slot 0 target_slot 1\b/,
		'shift-epoch update records its source and target');
}

my $node = PostgreSQL::Test::Cluster->new('umbra_checkpoint_capture');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_flush_after = 1
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
CREATE TABLE umbra_checkpoint_capture(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_checkpoint_capture VALUES (1, 'initial');
]);
my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_checkpoint_capture'::regclass);]);
my $main_path = $node->data_dir . "/$relpath";
my $filenode = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('umbra_checkpoint_capture'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_checkpoint_capture WHERE id = 1;]);

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach('umbra-checkpoint-after-shift-epoch', 'wait')]);
$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach(
         'umbra-mapping-after-wal-before-publish', 'wait')]);

my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-shift-epoch');
pass('checkpoint publishes its shift epoch before the fresh delay scan');

# The selector publication point is cached in the writer, so preload it before
# the actual update reaches the critical publication region.
my $writer = $node->background_psql('postgres');
my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$writer->query_until(
	qr/preload_started/,
	q(\echo preload_started
SELECT injection_points_run('umbra-mapping-after-wal-before-publish');
\echo preload_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$writer->query_until(qr/preload_done/, '');

my $marker = 'shift-epoch-marker';
$writer->query_until(
	qr/update_started/,
	qq(\echo update_started
UPDATE umbra_checkpoint_capture SET payload = '$marker' WHERE id = 1;
\echo update_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
pass('slot shift pauses after WAL and before selector publication');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-shift-epoch')]);
$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('fresh delay scan catches the post-epoch slot shift');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$writer->query_until(qr/update_done/, '');
$checkpoint->query_until(qr/checkpoint_done/, '');
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

assert_image_free_shift_wal(
	$node, $filenode, $target_block, $wal_start, $wal_end);

is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_checkpoint_capture WHERE id = 1;]),
	$marker, 'update and checkpoint complete after selector publication');
is(read_active_slot($map_path, $block_size, $target_block), 1,
	'checkpoint persists the published selector target');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 0)), $marker) >= 0,
	'C1 writes the selected page to the target selector predecessor');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 1)), $marker) < 0,
	'C1 leaves the published target dirty for the next checkpoint');

$node->safe_psql('postgres', 'CHECKPOINT');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 1)), $marker) >= 0,
	'C2 writes the retained page to the published target');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-shift-epoch')]);
$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach(
         'umbra-mapping-after-wal-before-publish')]);
$writer->quit;
$checkpoint->quit;
$node->stop;

done_testing();
