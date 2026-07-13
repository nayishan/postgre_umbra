# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that MAP superblocks remain resident until an explicit flush point.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub read_hex_bytes
{
	my ($path, $offset, $length) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, 0))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $length);
	defined($nread) or BAIL_OUT("could not read \"$path\": $!");
	$nread == $length
	  or BAIL_OUT("short read from \"$path\": read $nread of $length bytes");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return unpack('H*', $buffer);
}

sub write_bytes
{
	my ($path, $offset, $buffer) = @_;

	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, 0))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nwritten = syswrite($fh, $buffer);
	defined($nwritten) or BAIL_OUT("could not write \"$path\": $!");
	$nwritten == length($buffer)
	  or BAIL_OUT(
		"short write to \"$path\": wrote $nwritten of " . length($buffer) . " bytes");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('super_cache');
$node->init;
$node->append_conf('postgresql.conf', "checkpoint_timeout = '1h'");
$node->start;

$node->safe_psql('postgres',
	'CREATE TABLE umbra_super_cache (id integer) WITH (autovacuum_enabled = false)');
$node->safe_psql('postgres', 'CHECKPOINT');

my $relation_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_super_cache'::regclass)});
my $map_path = $node->data_dir . "/${relation_path}_map";
my $main_nblocks_offset = 40;
my $superblock_size = 512;
my $padding_sentinel = pack('C*', (0xA5) x 64);

write_bytes($map_path, $superblock_size, $padding_sentinel);

$node->safe_psql('postgres', 'INSERT INTO umbra_super_cache VALUES (1)');
is(read_hex_bytes($map_path, $main_nblocks_offset, 4),
	'00000000', 'logical EOF remains dirty in the resident superblock cache');

$node->safe_psql('postgres', 'CHECKPOINT');
isnt(read_hex_bytes($map_path, $main_nblocks_offset, 4),
	'00000000', 'checkpoint flushes the resident superblock');
is(
	read_hex_bytes($map_path, $superblock_size, length($padding_sentinel)),
	unpack('H*', $padding_sentinel),
	'superblock flush writes only its 512-byte sector');

$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_super_cache'),
	'1', 'relation remains readable after checkpoint and crash');

$node->stop;
done_testing();
