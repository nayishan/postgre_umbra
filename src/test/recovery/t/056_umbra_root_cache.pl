# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify resident MAP root updates, checkpoint output, and crash reload.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_LOGICAL_MAIN_OFFSET => 16;
use constant MAP_CRC_OFFSET => 60;
use constant MAP_ROOT_IMAGE_SIZE => 64;

sub sql_literal
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub u32_at
{
	my ($buffer, $offset) = @_;
	return unpack('L', substr($buffer, $offset, 4));
}

sub read_root
{
	my ($node, $map_path) = @_;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($map_path)
		  . ', 0, '
		  . MAP_ROOT_IMAGE_SIZE
		  . ", false), 'hex');");

	BAIL_OUT("short Umbra MAP root in $map_path")
	  unless length($hex) == 2 * MAP_ROOT_IMAGE_SIZE;
	return pack('H*', $hex);
}

sub check_root_crc
{
	my ($node, $root, $phase) = @_;
	my $payload_hex = unpack('H*', substr($root, 0, MAP_CRC_OFFSET));
	my $expected = 0 + $node->safe_psql(
		'postgres', "SELECT crc32c(decode('$payload_hex', 'hex'));");

	is(u32_at($root, MAP_CRC_OFFSET), $expected,
		"$phase: resident root output has a valid CRC");
}

sub relation_map_path
{
	my ($node, $relation) = @_;
	my $path = $node->safe_psql(
		'postgres',
		"SELECT pg_relation_filepath(" . sql_literal($relation) . "::regclass);");

	return "${path}_map";
}

my $node = PostgreSQL::Test::Cluster->new('root_cache');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
fsync = on
});
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE root_cache_checkpoint(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE root_cache_checkpoint ALTER COLUMN payload SET STORAGE PLAIN;
CHECKPOINT;
});

my $checkpoint_map = relation_map_path($node, 'root_cache_checkpoint');
is(u32_at(read_root($node, $checkpoint_map), MAP_LOGICAL_MAIN_OFFSET), 0,
	'new relation starts with an empty disk root');

$node->safe_psql(
	'postgres',
	q{INSERT INTO root_cache_checkpoint VALUES (1, repeat('x', 7000));});
is(u32_at(read_root($node, $checkpoint_map), MAP_LOGICAL_MAIN_OFFSET), 0,
	'committed extension remains only in the resident root cache');
is($node->safe_psql('postgres',
		'SELECT count(*) FROM root_cache_checkpoint;'),
	'1', 'another command reads the relation through the resident root');

$node->safe_psql('postgres', 'CHECKPOINT;');
my $checkpoint_root = read_root($node, $checkpoint_map);
cmp_ok(u32_at($checkpoint_root, MAP_LOGICAL_MAIN_OFFSET), '>', 0,
	'checkpoint writes the dirty resident root');
check_root_crc($node, $checkpoint_root, 'checkpoint');

$node->safe_psql(
	'postgres', q{
CREATE TABLE root_cache_crash(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE root_cache_crash ALTER COLUMN payload SET STORAGE PLAIN;
CHECKPOINT;
INSERT INTO root_cache_crash VALUES (2, repeat('y', 7000));
});
my $crash_map = relation_map_path($node, 'root_cache_crash');
is(u32_at(read_root($node, $crash_map), MAP_LOGICAL_MAIN_OFFSET), 0,
	'crash relation has only a dirty resident root before restart');

$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT id FROM root_cache_crash;'),
	'2', 'WAL and MAP redo recover a relation after losing the root cache');
$node->safe_psql('postgres', 'CHECKPOINT;');
my $recovered_root = read_root($node, $crash_map);
cmp_ok(u32_at($recovered_root, MAP_LOGICAL_MAIN_OFFSET), '>', 0,
	'recovered root is written at the next checkpoint');
check_root_crc($node, $recovered_root, 'recovery');

$node->safe_psql(
	'postgres', q{
CREATE TABLE root_cache_drop(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE root_cache_drop ALTER COLUMN payload SET STORAGE PLAIN;
CHECKPOINT;
INSERT INTO root_cache_drop VALUES (3, repeat('z', 7000));
DROP TABLE root_cache_drop;
CHECKPOINT;
});
pass('checkpoint does not write a dirty root after relation invalidation');

$node->stop;
done_testing();
