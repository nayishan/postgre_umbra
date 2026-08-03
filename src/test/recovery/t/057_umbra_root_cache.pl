# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a validated Umbra metadata root remains resident until restart.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('root_cache');
$node->init;
$node->start;

my $bootstrap_main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('pg_catalog.pg_class'::regclass);});
my $bootstrap_map_path = $node->data_dir . "/${bootstrap_main_path}_map";
ok(-e $bootstrap_map_path,
	'bootstrap-created pg_class has a metadata root');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_root_cache (id integer);
INSERT INTO umbra_root_cache VALUES (1);
CHECKPOINT;
});

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_root_cache'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";
my $crc = unpack('L', substr(slurp_file($map_path), 60, 4));

open(my $map_fh, '+<', $map_path)
  or die "could not open \"$map_path\": $!";
binmode($map_fh);
seek($map_fh, 60, 0)
  or die "could not seek \"$map_path\": $!";
print $map_fh pack('L', $crc ^ 1)
  or die "could not corrupt \"$map_path\": $!";
close($map_fh)
  or die "could not close \"$map_path\": $!";

is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_root_cache'),
	'1', 'a new backend uses the resident validated metadata root');

$node->stop;
$node->start;

my ($result, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT * FROM umbra_root_cache');
is($result, 3, 'restart reloads rather than trusting the former root cache');
like($stderr, qr/Umbra metadata root is corrupted or incompatible/,
	'restart validates the reloaded metadata root');

$node->stop;

done_testing();
