# Verify MAP superblock watermarks don't regress across crash restart.
#
# This test is UMBRA-specific. In md mode there is no MAP fork, so skip.
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

my $chunk_pages = 32;

sub chunk_capacity_for_nblocks
{
	my ($nblocks) = @_;

	return 0 if $nblocks == 0;
	return (int(($nblocks - 1) / $chunk_pages) + 1) * (2 * $chunk_pages);
}

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
});
$node->start();

$node->safe_psql(
	'postgres', q{
CREATE TABLE map_super_t(a int, b text);
INSERT INTO map_super_t
SELECT g, repeat('x', 400) FROM generate_series(1, 20000) g;
CHECKPOINT;
});

my $map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('map_super_t') || '_map', 0, 64, true), 'hex');}
);

my $logical_expected_1 = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('map_super_t') / current_setting('block_size')::int;}
);

my $magic_1 = u32le_from_hex($map_super_hex, 0);
my $version_1 = u32le_from_hex($map_super_hex, 4);
my $blcksz_1 = u32le_from_hex($map_super_hex, 8);
my $next_free_main_1 = u32le_from_hex($map_super_hex, 16);
my $phys_capacity_main_1 = u32le_from_hex($map_super_hex, 20);
my $logical_main_1 = u32le_from_hex($map_super_hex, 40);

is($magic_1, 0x554D4252, 'superblock magic matches UMBR');
is($version_1, 1, 'superblock version matches');
is($blcksz_1, 8192, 'superblock block size matches');
cmp_ok($logical_main_1, '==', $logical_expected_1,
	'logical_nblocks_main matches relation size in blocks');
cmp_ok($phys_capacity_main_1, '>=', chunk_capacity_for_nblocks($logical_main_1),
	'phys_capacity_main covers chunk-paired logical capacity');
cmp_ok($phys_capacity_main_1, '>=', $next_free_main_1,
	'phys_capacity_main not behind next_free_phys_block_main');

$node->safe_psql(
	'postgres', q{
INSERT INTO map_super_t
SELECT g, repeat('y', 400) FROM generate_series(20001, 40000) g;
CHECKPOINT;
});

$node->stop('immediate');
$node->start();

$map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('map_super_t') || '_map', 0, 64, true), 'hex');}
);

my $logical_expected_2 = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('map_super_t') / current_setting('block_size')::int;}
);

my $next_free_main_2 = u32le_from_hex($map_super_hex, 16);
my $phys_capacity_main_2 = u32le_from_hex($map_super_hex, 20);
my $logical_main_2 = u32le_from_hex($map_super_hex, 40);

cmp_ok($logical_main_2, '==', $logical_expected_2,
	'logical_nblocks_main survives crash restart');
cmp_ok($logical_main_2, '>=', $logical_main_1,
	'logical_nblocks_main does not regress');
cmp_ok($next_free_main_2, '>=', $next_free_main_1,
	'next_free_phys_block_main does not regress');
cmp_ok($phys_capacity_main_2, '>=', $phys_capacity_main_1,
	'phys_capacity_main does not regress');
cmp_ok($phys_capacity_main_2, '>=', $next_free_main_2,
	'phys_capacity_main remains ahead of next_free_phys_block_main');

done_testing();
