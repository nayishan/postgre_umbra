# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that Umbra keeps mapped data and MAP as one delayed-unlink unit.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('mainfork_unlink');
$node->init;
$node->append_conf('postgresql.conf', "checkpoint_timeout = '1h'");
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_unlink_test (id integer);
INSERT INTO umbra_unlink_test VALUES (1);
});

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_unlink_test'::regclass);});
my $map_path = "${main_path}_map";

my $size_sql =
  "SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1);";
my $map_size_sql =
  "SELECT COALESCE((pg_stat_file('$map_path', true)).size, -1);";
my $main_size_before = $node->safe_psql('postgres', $size_sql);
my $map_size_before = $node->safe_psql('postgres', $map_size_sql);

cmp_ok(
	$main_size_before,
	'>',
	0,
	'MAIN segment 0 contains relation data before DROP');
cmp_ok($map_size_before, '>', 0, 'MAP contains mappings before DROP');

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', 'DROP TABLE umbra_unlink_test');

is($node->safe_psql('postgres', $size_sql), $main_size_before,
	'mapped MAIN remains intact before checkpoint');
is($node->safe_psql('postgres', $map_size_sql), $map_size_before,
	'MAP remains intact with mapped MAIN before checkpoint');

$node->safe_psql('postgres', 'CHECKPOINT');

is($node->safe_psql('postgres', $size_sql), '-1',
	'checkpoint removes the delayed-unlink MAIN segment 0');
is($node->safe_psql('postgres', $map_size_sql), '-1',
	'checkpoint removes the delayed-unlink MAP');

$node->stop;
done_testing();
