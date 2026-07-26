# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that CREATE redo reconstructs a damaged private metadata root.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('map_root_redo');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_redo_root (id integer);
INSERT INTO umbra_redo_root VALUES (42);
});

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_redo_root'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";
my $root = slurp_file($map_path);
my $crc = unpack('L', substr($root, 60, 4));

# Leave CREATE and INSERT after the prior checkpoint, then force crash redo.
$node->stop('immediate');

open(my $corrupt_fh, '+<', $map_path)
  or die "could not open \"$map_path\": $!";
binmode($corrupt_fh);
seek($corrupt_fh, 60, 0)
  or die "could not seek \"$map_path\": $!";
print $corrupt_fh pack('L', $crc ^ 1)
  or die "could not corrupt \"$map_path\": $!";
close($corrupt_fh)
  or die "could not close \"$map_path\": $!";

$node->start;
is($node->safe_psql('postgres', 'SELECT id FROM umbra_redo_root'), '42',
	'CREATE redo restores the relation after a damaged metadata root');

$root = slurp_file($map_path);
my ($magic, $version) = unpack('L2', substr($root, 0, 8));
is($magic, 0x554d4252, 'CREATE redo reconstructs root magic');
is($version, 3, 'CREATE redo reconstructs root version');
is(substr($root, 64, 448), "\0" x 448,
	'CREATE redo reconstructs zeroed root sector padding');

$node->stop;
done_testing();
