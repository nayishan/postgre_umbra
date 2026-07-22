# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify bootstrap and lifecycle ownership of Umbra's private metadata root.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('map_root');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_root_test (id integer);
INSERT INTO umbra_root_test VALUES (1);
CHECKPOINT;
});

my $block_size = $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_root_test'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";
my $root = slurp_file($map_path);
my ($magic, $version, $blcksz, $chunk_pages) =
  unpack('L4', substr($root, 0, 16));

is(length($root), $block_size, 'metadata root occupies one regular block');
is($magic, 0x554d4252, 'metadata root magic is present');
is($version, 1, 'metadata root format version is present');
is($blcksz, $block_size, 'metadata root records BLCKSZ');
is($chunk_pages, 32, 'metadata root records the fixed chunk size');
is(substr($root, 64, 448), "\0" x 448,
	'metadata root sector padding is zeroed');

$node->safe_psql('postgres', 'CREATE UNLOGGED TABLE umbra_root_unlogged (id integer)');
my $unlogged_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_root_unlogged'::regclass);});
ok(!-e $node->data_dir . "/${unlogged_path}_map",
	'unlogged relation does not acquire WAL-owned metadata');

$node->safe_psql('postgres', 'DROP TABLE umbra_root_test');
ok(!-e $map_path, 'dropping MAIN removes the private metadata file');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_root_corrupt (id integer);
INSERT INTO umbra_root_corrupt VALUES (1);
CHECKPOINT;
});
my $corrupt_main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_root_corrupt'::regclass);});
my $corrupt_map_path = $node->data_dir . "/${corrupt_main_path}_map";
my $corrupt_crc = unpack('L', substr(slurp_file($corrupt_map_path), 60, 4));

$node->stop;

open(my $corrupt_fh, '+<', $corrupt_map_path)
  or die "could not open \"$corrupt_map_path\": $!";
binmode($corrupt_fh);
seek($corrupt_fh, 60, 0)
  or die "could not seek \"$corrupt_map_path\": $!";
print $corrupt_fh pack('L', $corrupt_crc ^ 1)
  or die "could not corrupt \"$corrupt_map_path\": $!";
close($corrupt_fh)
  or die "could not close \"$corrupt_map_path\": $!";

$node->start;
my ($result, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT * FROM umbra_root_corrupt');
is($result, 3, 'corrupt metadata root prevents relation access');
like($stderr, qr/Umbra metadata root is corrupted or incompatible/,
	'metadata root CRC is validated after restart');
$node->stop;

done_testing();
