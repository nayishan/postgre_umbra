# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that unlogged relations use direct layout without selector MAP.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_unlogged_selector');
$node->init(no_data_checksums => 1);
$node->start;

$node->safe_psql('postgres', q{
CREATE UNLOGGED TABLE umbra_unlogged_selector (id integer);
INSERT INTO umbra_unlogged_selector VALUES (1);
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_unlogged_selector'::regclass);});
my $main_file = $node->data_dir . "/$main_path";
my $map_path = $node->data_dir . "/${main_path}_map";

is(-s $main_file, $block_size,
	'unlogged MAIN uses direct physical layout');
ok(!-e $map_path, 'unlogged MAIN has no selector-MAP file');

$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres',
		'SELECT count(*) FROM umbra_unlogged_selector'), '0',
	'unlogged contents are restored from INIT after a crash');
ok(!-e $map_path, 'unlogged reset leaves no selector-MAP file');

$node->safe_psql('postgres',
	'INSERT INTO umbra_unlogged_selector VALUES (2); CHECKPOINT;');
ok(!-e $map_path, 'later unlogged writes still avoid selector metadata');

$node->stop;
done_testing();
