# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that the mapwriter cleans ordinary MAP pages and that physical
# preallocation publishes only addressable capacity.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub read_bytes
{
	my ($path, $offset, $length) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $length);
	defined($nread) && $nread == $length
	  or BAIL_OUT("could not read $length bytes from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
	return $buffer;
}

my $node = PostgreSQL::Test::Cluster->new('umbra_mapwriter');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
mapwriter_delay = '20ms'
mapwriter_lru_maxpages = 0
mapwriter_prealloc_max_relations = 0
map_prealloc_main_low = 64
map_prealloc_main_hard = 1
map_prealloc_main_batch = 128
});
$node->start;

ok($node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'map writer'},
		't'),
	'map writer is visible as an independent background worker');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_mapwriter_t(id integer, payload text)
  WITH (autovacuum_enabled = false);
CHECKPOINT;
});
my $relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_mapwriter_t')});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'))});
my $main_entry_zero_offset = 3 * $block_size;

$node->safe_psql(
	'postgres', q{INSERT INTO umbra_mapwriter_t VALUES (1, 'dirty-map-page')});

is(unpack('H*', read_bytes($map_path, $main_entry_zero_offset, 4)),
	'ffffffff', 'disabled mapwriter leaves the dirty MAP entry in memory');

$node->safe_psql(
	'postgres', q{ALTER SYSTEM SET mapwriter_lru_maxpages = 100});
$node->safe_psql('postgres', q{SELECT pg_reload_conf()});
my $map_cleaned = 0;
for (1 .. 200)
{
	if (unpack('H*', read_bytes($map_path, $main_entry_zero_offset, 4)) eq
		'00000000')
	{
		$map_cleaned = 1;
		last;
	}
	usleep(50_000);
}
ok($map_cleaned,
	'mapwriter flushes an ordinary MAP page without an explicit checkpoint');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_prealloc_t(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_prealloc_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_prealloc_t
SELECT g, repeat('x', 7000) FROM generate_series(1, 130) AS g;
CHECKPOINT;
});
my $prealloc_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_prealloc_t')});
my $prealloc_data = $node->data_dir . "/$prealloc_path";
my $prealloc_map = "${prealloc_data}_map";
my $root_before = read_bytes($prealloc_map, 0, 64);
my $logical_before = unpack('L', substr($root_before, 16, 4));
my $frontier_before = unpack('L', substr($root_before, 28, 4));
my $capacity_before = unpack('L', substr($root_before, 48, 4));
my $file_blocks_before = (-s $prealloc_data) / $block_size;

cmp_ok($capacity_before - $frontier_before, '>', 1,
	'foreground leaves more than the hard preallocation reserve');
cmp_ok($capacity_before - $frontier_before, '<=', 64,
	'relation is eligible for background preallocation');

$node->safe_psql(
	'postgres', q{ALTER SYSTEM SET mapwriter_prealloc_max_relations = 32});
$node->safe_psql('postgres', q{SELECT pg_reload_conf()});
my $worker_grew_file = 0;
for (1 .. 200)
{
	if ((-s $prealloc_data) / $block_size > $file_blocks_before)
	{
		$worker_grew_file = 1;
		last;
	}
	usleep(50_000);
}
ok($worker_grew_file, 'mapwriter preallocates physical capacity');

$node->safe_psql('postgres', 'CHECKPOINT');
my $root = read_bytes($prealloc_map, 0, 64);
my $logical = unpack('L', substr($root, 16, 4));
my $frontier = unpack('L', substr($root, 28, 4));
my $capacity = unpack('L', substr($root, 48, 4));
my $file_blocks = (-s $prealloc_data) / $block_size;

cmp_ok($logical, '>', 0, 'preallocation test relation has a logical range');
is($logical, $logical_before,
	'preallocation does not change the logical EOF');
is($frontier, $frontier_before,
	'preallocation does not reserve physical blocks');
cmp_ok($capacity, '>', $capacity_before,
	'mapwriter publishes the larger physical capacity');
cmp_ok($capacity, '>=', $frontier,
	'published capacity never trails the allocation frontier');
cmp_ok($file_blocks, '>=', $capacity,
	'the physical file covers every published capacity block');
cmp_ok($capacity, '>', $frontier,
	'native preallocation keeps capacity ahead of allocation');

$node->stop('immediate');
$node->start;
my $root_after_restart = read_bytes($prealloc_map, 0, 64);
is(unpack('L', substr($root_after_restart, 28, 4)), $frontier,
	'crash restart preserves the allocation frontier');
is(unpack('L', substr($root_after_restart, 48, 4)), $capacity,
	'crash restart preserves the independently published capacity');

$node->stop;
done_testing();
