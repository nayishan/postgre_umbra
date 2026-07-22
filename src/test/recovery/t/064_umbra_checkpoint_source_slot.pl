# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a checkpoint writes a buffer shifted after selection to its
# captured source slot, leaving the current slot dirty for the next checkpoint.

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

my $node = PostgreSQL::Test::Cluster->new('umbra_checkpoint_source_slot');
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
CREATE TABLE umbra_checkpoint_source_slot(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_checkpoint_source_slot VALUES (1, 'initial');
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_checkpoint_source_slot'::regclass);]);
my $main_path = $node->data_dir . "/$relpath";
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_checkpoint_source_slot WHERE id = 1;]);

$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'initial checkpoint persists slot 0');

my $first_block = 0 + $node->safe_psql(
	'postgres', q[
UPDATE umbra_checkpoint_source_slot SET payload = 'slot-one' WHERE id = 1
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
pass('C1 flushes selector slot 1 before buffer writeback');

my $marker = 'p11-source-slot-marker';
my $second_block = 0 + $node->safe_psql(
	'postgres', qq[
UPDATE umbra_checkpoint_source_slot SET payload = '$marker' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]);
is($second_block, $target_block,
	'C1-overlap update remains on the selected logical block');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-map')]);
$checkpoint->query_until(qr/checkpoint_done/, '');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-map')]);

is(read_active_slot($map_path, $block_size, $target_block), 1,
	'C1 leaves its on-disk selector at the captured source slot');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 1)), $marker) >= 0,
	'C1 writes the post-shift page image to source slot 1');

$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $target_block), 2,
	'C2 persists the current selector slot');
ok(index(read_physical_block($main_path, $block_size,
			source_slot_block($target_block, 2)), $marker) >= 0,
	'C2 writes the retained dirty page to current slot 2');
is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_checkpoint_source_slot WHERE id = 1;]),
	$marker, 'logical read follows the current selector');

$checkpoint->quit;
$node->stop;

done_testing();
