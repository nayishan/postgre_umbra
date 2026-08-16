# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that selector-cache eviction persists a dirty selector without a
# relation-level root.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $page_index = int($logical_block / $entries_per_page);
	my $entry_index = $logical_block % $entries_per_page;
	my $group = int($page_index / 256);
	my $map_block = $group * 258 + 2 + ($page_index % 256);
	my $offset = $map_block * $block_size + int($entry_index / 4);
	my $byte;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	sysread($fh, $byte, 1) == 1
	  or BAIL_OUT("could not read selector from \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return (unpack('C', $byte) >> (($entry_index % 4) * 2)) & 0x03;
}

my $node = PostgreSQL::Test::Cluster->new('map_cache');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q[
shared_buffers = '128kB'
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
mapwriter_lru_maxpages = 0
max_wal_size = '4GB'
]);
$node->start;

# The pool minimum is 16 slots.  Give each table one distinct MAIN selector
# page, then restart so the update sequence begins with an empty selector cache.
$node->safe_psql(
	'postgres', q[
DO $$
BEGIN
  FOR i IN 0..16 LOOP
    EXECUTE format(
      'CREATE TABLE umbra_cache_%s (id integer, payload text) WITH (autovacuum_enabled = false)',
      i);
    EXECUTE format('INSERT INTO umbra_cache_%s VALUES (1, ''initial'')', i);
  END LOOP;
END
$$;
CHECKPOINT;
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer FROM umbra_cache_0 WHERE id = 1]);
my $target_path = $node->safe_psql(
	'postgres', q[
SELECT pg_relation_filepath('umbra_cache_0'::regclass)]);
my $target_map = $node->data_dir . "/${target_path}_map";

is(-s $target_map, 3 * $block_size,
	'new relation has only the three selector pages in its first MAP group');
is(read_active_slot($target_map, $block_size, $target_block), 0,
	'new selector defaults to slot 0');

$node->stop;
$node->start;
$node->safe_psql('postgres', 'CHECKPOINT');

my $updated_block = 0 + $node->safe_psql(
	'postgres', q[
UPDATE umbra_cache_0 SET payload = 'shifted' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer]);
is($updated_block, $target_block, 'target update stays on its original page');
is(read_active_slot($target_map, $block_size, $target_block), 0,
	'dirty selector has not been written before cache eviction');

# Loading 16 additional relation selectors fills the 16-slot pool and evicts
# the first dirty entry.  Map writer and checkpoints are disabled above, so
# this write is attributable to the cache replacement path.
for my $i (1 .. 16)
{
	$node->safe_psql(
		'postgres',
		"UPDATE umbra_cache_$i SET payload = 'shifted' WHERE id = 1");
}
is(read_active_slot($target_map, $block_size, $target_block), 1,
	'cache eviction writes the first dirty selector page');

$node->stop('immediate');
$node->start;
is(read_active_slot($target_map, $block_size, $target_block), 1,
	'evicted selector survives crash recovery');
is($node->safe_psql(
	'postgres', q[
SELECT payload FROM umbra_cache_0 WHERE id = 1]),
	'shifted', 'recovery follows the persisted selector');

$node->stop;
done_testing();
