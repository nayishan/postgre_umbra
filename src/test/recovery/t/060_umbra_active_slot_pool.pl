# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify default slot-0 selectors, pool replacement, and selector corruption.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub append_zero_block
{
	my ($path, $block_size) = @_;
	my $page = "\0" x $block_size;

	open(my $fh, '>>', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	print $fh $page or BAIL_OUT("could not append to \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

sub write_byte
{
	my ($path, $offset, $value) = @_;

	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek \"$path\": $!");
	print $fh pack('C', $value)
	  or BAIL_OUT("could not write \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

my $node = PostgreSQL::Test::Cluster->new('active_slot_pool');
$node->init;
$node->append_conf(
	'postgresql.conf',
	"shared_buffers = '128kB'\nautovacuum = off\ncheckpoint_timeout = '1h'\nmax_wal_size = '4GB'");
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_selector_anchor (id integer) WITH (autovacuum_enabled = false);
INSERT INTO umbra_selector_anchor VALUES (1);
DO $$
BEGIN
  FOR i IN 1..24 LOOP
    EXECUTE format(
      'CREATE TABLE umbra_selector_pool_%s (id integer) WITH (autovacuum_enabled = false)',
      i);
    EXECUTE format('INSERT INTO umbra_selector_pool_%s VALUES (1)', i);
  END LOOP;
END
$$;
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my @relations = ('umbra_selector_anchor',
	map { "umbra_selector_pool_$_" } 1 .. 24);
my @map_paths;

for my $relation (@relations)
{
	my $main_path = $node->safe_psql(
		'postgres', "SELECT pg_relation_filepath('$relation'::regclass)");
	push @map_paths, $node->data_dir . "/${main_path}_map";
}

is(-s $map_paths[0], $block_size,
	'new relation has only its metadata root before a selector is published');

# A restart forces a physical read through MapGetActiveSlot().
$node->stop;
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_selector_anchor'),
	'1', 'an absent selector page reads as slot 0');
is(-s $map_paths[0], $block_size,
	'ordinary slot-0 reads do not materialize a selector page');

$node->stop;
for my $map_path (@map_paths)
{
	append_zero_block($map_path, $block_size);
}
$node->start;

# The pool minimum is 16 slots; 25 relation tags force replacement.
for my $relation (@relations)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM $relation"), '1',
		"zero selector page keeps $relation on slot 0");
}
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop;
$node->start;
for my $relation (@relations)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM $relation"), '1',
		"selector cache replacement survives restart for $relation");
}

$node->stop;
write_byte($map_paths[0], $block_size, 3);
$node->start;
my ($result, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT count(*) FROM umbra_selector_anchor');
is($result, 3, 'invalid selector rejects data access');
like($stderr, qr/invalid Umbra active slot 3/,
	'invalid selector is reported as metadata corruption');

$node->stop;

done_testing();
