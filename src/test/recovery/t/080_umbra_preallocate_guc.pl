# Verify Umbra MAIN-fork preallocation publishes capacity only after the
# underlying file has been extended to cover it.
#
# In md mode, skip this test.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub u32le_from_hex
{
	my ($hex, $offset) = @_;
	my $chunk = substr($hex, $offset * 2, 8);
	my @b = ($chunk =~ /../g);

	return hex($b[0]) +
	  (hex($b[1]) << 8) +
	  (hex($b[2]) << 16) +
	  (hex($b[3]) << 24);
}

my $node = PostgreSQL::Test::Cluster->new('umbra_preallocate_guc');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
map_prealloc_main_low = 64
map_prealloc_main_hard = 64
map_prealloc_main_batch = 128
});
$node->start();

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_prealloc_t(id int, payload text);
ALTER TABLE umbra_prealloc_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_prealloc_t
SELECT g, repeat('x', 7000) FROM generate_series(1, 2000) g;
CHECKPOINT;
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_prealloc_t');}
);

my $map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umbra_prealloc_t') || '_map', 0, 64, true), 'hex');}
);

my $next_free_main = u32le_from_hex($map_super_hex, 16);
my $phys_capacity_main = u32le_from_hex($map_super_hex, 20);
my $logical_main = u32le_from_hex($map_super_hex, 40);
my $main_file_blocks = $node->safe_psql(
	'postgres',
	"SELECT ((pg_stat_file('$main_path')).size / current_setting('block_size')::int)::bigint;");

cmp_ok($logical_main, '>', 0, 'table has non-zero logical size');
cmp_ok($next_free_main, '>=', $logical_main,
	'next_free_phys_block_main covers logical blocks');
cmp_ok($phys_capacity_main, '>', $next_free_main,
	'GUC-driven preallocation keeps capacity ahead of next_free');
cmp_ok($main_file_blocks, '>=', $phys_capacity_main,
	'MAIN fork file size covers published physical capacity');

$node->stop;

done_testing();
