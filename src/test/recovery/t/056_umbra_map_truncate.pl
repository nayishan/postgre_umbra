# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that lazy VACUUM shrinks Umbra's physical MAIN fork and frontiers.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

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

sub read_map_super
{
	my ($node, $map_path) = @_;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($map_path)
		  . ", 0, 64, false), 'hex');");

	BAIL_OUT('short Umbra MAP superblock') unless length($hex) == 128;
	return pack('H*', $hex);
}

sub physical_main_bytes
{
	my ($node, $main_path) = @_;
	my $bytes = 0;
	my $segments = 0;

	for (my $segno = 0;; $segno++)
	{
		my $suffix = $segno == 0 ? '' : ".$segno";
		my $file = $node->data_dir . "/$main_path$suffix";

		last unless -e $file;
		my $segment_bytes = -s $file;
		BAIL_OUT("could not stat physical MAIN segment \"$file\"")
		  unless defined($segment_bytes);
		$bytes += $segment_bytes;
		$segments++;
	}

	BAIL_OUT("missing physical MAIN fork \"$main_path\"")
	  unless $segments > 0;
	return $bytes;
}

sub check_main_frontiers
{
	my ($super, $physical_blocks, $phase) = @_;

	is(u32_at($super, 16), $physical_blocks,
		"$phase: next-free MAIN frontier matches physical EOF");
	is(u32_at($super, 20), $physical_blocks,
		"$phase: MAIN physical capacity matches physical EOF");
	is(u32_at($super, 40), $physical_blocks,
		"$phase: MAIN logical EOF matches physical EOF");
}

my $node = PostgreSQL::Test::Cluster->new('map_truncate');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
fsync = on
});
$node->start;

my $block_size = 0 + $node->safe_psql(
	'postgres',
	q{SELECT pg_size_bytes(current_setting('block_size'));});
my $row_count = 1500;
my $remaining_rows = 100;
my $payload_repeats = int($block_size / 64);

$node->safe_psql(
	'postgres', q{
CREATE TABLE umb_map_truncate_t(id int, payload text)
  WITH (autovacuum_enabled = false, vacuum_truncate = true);
ALTER TABLE umb_map_truncate_t ALTER COLUMN payload SET STORAGE PLAIN;
});
$node->safe_psql(
	'postgres', qq{
INSERT INTO umb_map_truncate_t
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, $row_count) AS g
ORDER BY g;
});
$node->safe_psql('postgres', 'CHECKPOINT;');

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_map_truncate_t'::regclass);});
my $map_path = "${main_path}_map";
my $bytes_before = physical_main_bytes($node, $main_path);

is($bytes_before % $block_size, 0,
	'initial physical MAIN file is block aligned');
my $blocks_before = int($bytes_before / $block_size);
is($blocks_before, $row_count,
	'one PLAIN half-page tuple occupies each initial MAIN block');
my $super_before = read_map_super($node, $map_path);
check_main_frontiers($super_before, $blocks_before, 'before VACUUM');

$node->safe_psql(
	'postgres',
	"DELETE FROM umb_map_truncate_t WHERE id > $remaining_rows;");
$node->safe_psql(
	'postgres',
	'VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umb_map_truncate_t;');

my $main_path_after = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_map_truncate_t'::regclass);});
is($main_path_after, $main_path,
	'lazy VACUUM keeps the same relation file');

my $bytes_after = physical_main_bytes($node, $main_path_after);
cmp_ok($bytes_after, '<', $bytes_before,
	'lazy VACUUM shortens the physical MAIN file');
is($bytes_after % $block_size, 0,
	'truncated physical MAIN file is block aligned');
my $blocks_after = int($bytes_after / $block_size);
cmp_ok($blocks_after, '<', $blocks_before,
	'physical MAIN EOF moves backward');

my $logical_after = 0 + $node->safe_psql(
	'postgres',
	"SELECT pg_relation_size('umb_map_truncate_t') / $block_size;");
is($logical_after, $blocks_after,
	'logical MAIN size matches the shortened physical file');

my $super_after = read_map_super($node, $map_path);
check_main_frontiers($super_after, $blocks_after, 'after VACUUM');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*), min(id), max(id) FROM umb_map_truncate_t;}),
	"$remaining_rows|1|$remaining_rows",
	'rows outside the truncated tail remain readable');

$node->safe_psql('postgres', 'CHECKPOINT;');
my $super_checkpoint_hex = unpack('H*', read_map_super($node, $map_path));
$node->stop('immediate');
$node->start;

is(
	$node->safe_psql(
		'postgres',
		q{SELECT pg_relation_filepath('umb_map_truncate_t'::regclass);}),
	$main_path,
	'relation file path survives immediate restart');
is(physical_main_bytes($node, $main_path), $bytes_after,
	'physical MAIN shrink survives immediate restart');

my $super_restart = read_map_super($node, $map_path);
is(unpack('H*', $super_restart), $super_checkpoint_hex,
	'truncated superblock survives immediate restart');
check_main_frontiers($super_restart, $blocks_after, 'after restart');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*), min(id), max(id) FROM umb_map_truncate_t;}),
	"$remaining_rows|1|$remaining_rows",
	'remaining rows survive immediate restart');

$node->stop;
done_testing();
