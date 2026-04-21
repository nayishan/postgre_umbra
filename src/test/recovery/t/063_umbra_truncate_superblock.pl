# Verify TRUNCATE updates MAP superblock logical_nblocks and survives restart.
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

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
});
$node->start();

$node->safe_psql(
	'postgres', q{
CREATE TABLE umb_truncate_t(a int, b text);
INSERT INTO umb_truncate_t
SELECT g, repeat('t', 400) FROM generate_series(1, 20000) g;
CHECKPOINT;
});

my $map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_truncate_t') || '_map', 0, 64, true), 'hex');}
);

my $logical_before = u32le_from_hex($map_super_hex, 40);
cmp_ok($logical_before, '>', 0, 'logical_nblocks_main is non-zero before TRUNCATE');

$node->safe_psql(
	'postgres', q{
TRUNCATE umb_truncate_t;
CHECKPOINT;
});

my $logical_size_after = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('umb_truncate_t') / current_setting('block_size')::int;}
);

$map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_truncate_t') || '_map', 0, 64, true), 'hex');}
);
my $logical_after = u32le_from_hex($map_super_hex, 40);

is($logical_size_after, '0', 'relation size is zero blocks after TRUNCATE');
is($logical_after, 0, 'superblock logical_nblocks_main is zero after TRUNCATE');

$node->stop('immediate');
$node->start();

$map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_truncate_t') || '_map', 0, 64, true), 'hex');}
);
my $logical_after_restart = u32le_from_hex($map_super_hex, 40);

is($logical_after_restart, 0,
	'superblock logical_nblocks_main remains zero after restart');

done_testing();
