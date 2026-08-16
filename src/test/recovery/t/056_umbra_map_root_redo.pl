# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that physical length is the only logical-EOF authority.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('three_bucket_length');
$node->init(no_data_checksums => 1);
$node->start;
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_three_bucket_length (id integer);
INSERT INTO umbra_three_bucket_length VALUES (42);
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_three_bucket_length'::regclass);});
my $main_file = $node->data_dir . "/$main_path";
my $logical_blocks = 0 + $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('umbra_three_bucket_length'::regclass) /
          current_setting('block_size')::integer;});

is(-s $main_file, 3 * $logical_blocks * $block_size,
	'data fork stores exactly three physical blocks per logical block');

$node->stop;
open(my $fh, '>>', $main_file)
  or BAIL_OUT("could not open \"$main_file\": $!");
binmode($fh);
print $fh "\0" x $block_size
  or BAIL_OUT("could not append to \"$main_file\": $!");
close($fh) or BAIL_OUT("could not close \"$main_file\": $!");

$node->start;
is($node->safe_psql('postgres',
		'SELECT count(*) FROM umbra_three_bucket_length'), '1',
	'a partial physical tail preserves the completed logical prefix');
is(0 + $node->safe_psql('postgres',
		q{SELECT pg_relation_size('umbra_three_bucket_length'::regclass) /
			  current_setting('block_size')::integer;}), $logical_blocks,
	'incomplete slots do not advance the logical EOF');

$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_three_bucket_length
SELECT generate_series(1, 10000);
CHECKPOINT;
});
my $grown_blocks = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_relation_size('umbra_three_bucket_length'::regclass) /
		current_setting('block_size')::integer;});
cmp_ok($grown_blocks, '>', $logical_blocks,
	'extension grows past the incomplete tail');
is(-s $main_file, 3 * $grown_blocks * $block_size,
	'extension completes the trailing three-slot bucket');

$node->stop;
done_testing();
