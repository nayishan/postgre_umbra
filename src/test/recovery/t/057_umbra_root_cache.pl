# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that unlogged relations never create selector metadata.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('unlogged_selector_reset');
$node->init(no_data_checksums => 1);
$node->start;
$node->safe_psql(
	'postgres', q{
CREATE UNLOGGED TABLE umbra_unlogged_selector (id integer PRIMARY KEY);
INSERT INTO umbra_unlogged_selector VALUES (1);
CHECKPOINT;
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_unlogged_selector'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";

ok(!-e $map_path,
	'unlogged MAIN has no selector-MAP file');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
		'SELECT count(*) FROM umbra_unlogged_selector'), '0',
	'unlogged contents are restored from INIT after a crash');
ok(!-e $map_path,
	'unlogged reset leaves no selector-MAP file');

$node->safe_psql('postgres',
	'INSERT INTO umbra_unlogged_selector VALUES (2)');
ok(!-e $map_path,
	'new unlogged MAIN remains without selector metadata');

$node->stop;
done_testing();
