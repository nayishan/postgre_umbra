# Verify UMBRA truncate/drop behavior across crash restart.
#
# Matrix intent:
# - TRUNCATE result survives crash restart (logical size and superblock logical_nblocks)
# - DROP result survives crash restart
# - dropped relation MAP fork disappears after a post-restart checkpoint
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
CREATE TABLE umb_mx_trunc_t(id int, payload text);
INSERT INTO umb_mx_trunc_t
SELECT g, repeat('a', 500) FROM generate_series(1, 18000) g;
CREATE TABLE umb_mx_drop_t(id int, payload text);
INSERT INTO umb_mx_drop_t
SELECT g, repeat('b', 500) FROM generate_series(1, 18000) g;
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_mx_trunc_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

cmp_ok(
	$node->safe_psql(
		'postgres',
		q{SELECT pg_relation_size('umb_mx_trunc_t') / current_setting('block_size')::int;}),
	'>',
	0,
	'truncate relation logical size is non-zero before TRUNCATE');

my $drop_map_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_mx_drop_t') || '_map';}
);

$node->safe_psql(
	'postgres', q{
TRUNCATE umb_mx_trunc_t;
DROP TABLE umb_mx_drop_t;
});

$node->stop('immediate');
$node->start();

is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_mx_trunc_t;}), '0',
	'TRUNCATE result survives crash restart');
is($node->safe_psql(
		'postgres',
		q{SELECT pg_relation_size('umb_mx_trunc_t') / current_setting('block_size')::int;}),
	'0',
	'truncated relation logical size is zero blocks after restart');

my $trunc_map_hex_after = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_mx_trunc_t') || '_map', 0, 64, true), 'hex');}
);
my $trunc_logical_after = u32le_from_hex($trunc_map_hex_after, 40);
is($trunc_logical_after, 0,
	'superblock logical_nblocks_main remains zero after crash restart');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_class WHERE relname = 'umb_mx_drop_t';}),
	'0',
	'DROP result survives crash restart');

$node->safe_psql('postgres', q{CHECKPOINT;});
ok($node->poll_query_until('postgres',
		"SELECT COALESCE(encode(pg_read_binary_file('$drop_map_path', 0, 1, true), 'hex'), '') = '';",
		't'),
	'dropped relation MAP fork disappears after post-restart checkpoint');

$node->safe_psql(
	'postgres', q{
INSERT INTO umb_mx_trunc_t
SELECT g, repeat('c', 300) FROM generate_series(1, 1000) g;
});
is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_mx_trunc_t;}), '1000',
	'truncated relation remains writable after crash restart');

done_testing();
