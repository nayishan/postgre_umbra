# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the rootless selector-MAP layout.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('no_root_layout');
$node->init;
$node->start;
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_no_root_layout (id integer);
INSERT INTO umbra_no_root_layout VALUES (1);
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_no_root_layout'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";
my $map = slurp_file($map_path);

is(length($map), 3 * $block_size,
	'first selector group has FSM, VM, and MAIN pages without a root page');
is(substr($map, 0, 4), "\0" x 4,
	'MAP block zero is a zero FSM selector, not a metadata header');

$node->safe_psql('postgres', 'DROP TABLE umbra_no_root_layout');
ok(!-e $map_path, 'dropping MAIN removes its private selector-MAP file');

$node->stop;
done_testing();
