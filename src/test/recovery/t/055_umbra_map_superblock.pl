# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the Umbra MAP superblock format, identity frontiers, and CRC.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_MAGIC => 0x554D4252;
use constant MAP_VERSION => 1;
use constant MAP_CORRUPTION_OFFSET => 52;

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

sub raw_fork_blocks
{
	my ($node, $main_path, $suffix, $block_size, $label) = @_;
	my $path = $main_path . $suffix;
	my $bytes = 0;
	my $segments = 0;

	for (my $segno = 0;; $segno++)
	{
		my $segment_suffix = $segno == 0 ? '' : ".$segno";
		my $file = $node->data_dir . "/$path$segment_suffix";

		last unless -e $file;
		my $segment_bytes = -s $file;
		BAIL_OUT("could not stat physical fork segment \"$file\"")
		  unless defined($segment_bytes);
		$bytes += $segment_bytes;
		$segments++;
	}

	BAIL_OUT("missing physical fork \"$path\"") unless $segments > 0;

	is($bytes % $block_size, 0,
		"$label physical file is block aligned");
	return int($bytes / $block_size);
}

sub check_format
{
	my ($super, $block_size, $phase) = @_;

	is(u32_at($super, 0), MAP_MAGIC, "$phase: superblock magic is UMBR");
	is(u32_at($super, 4), MAP_VERSION,
		"$phase: superblock version is supported");
	is(u32_at($super, 8), $block_size,
		"$phase: superblock block size matches the build");
	is(u32_at($super, 12), 0, "$phase: superblock flags are clear");
	is(substr($super, 52, 8), "\0" x 8,
		"$phase: reserved bytes are clear");
}

sub check_identity_frontiers
{
	my ($node, $super, $main_path, $block_size, $phase) = @_;
	my @forks = (
		[ 'main', '', 16, 20, 40 ],
		[ 'fsm', '_fsm', 24, 28, 44 ],
		[ 'vm', '_vm', 32, 36, 48 ]);
	my %blocks;

	for my $fork (@forks)
	{
		my ($name, $suffix, $next_offset, $capacity_offset,
			$logical_offset) = @{$fork};
		my $raw = raw_fork_blocks(
			$node, $main_path, $suffix, $block_size,
			"$phase: $name");

		$blocks{$name} = $raw;
		cmp_ok($raw, '>', 0, "$phase: $name physical fork exists");
		is(u32_at($super, $logical_offset), $raw,
			"$phase: $name logical EOF matches physical blocks");
		is(u32_at($super, $next_offset), $raw,
			"$phase: $name next-free frontier is identity");
		is(u32_at($super, $capacity_offset), $raw,
			"$phase: $name physical capacity is identity");
	}

	return \%blocks;
}

my $node = PostgreSQL::Test::Cluster->new('map_superblock');
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

$node->safe_psql(
	'postgres', q{
CREATE TABLE map_super_t(id int, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO map_super_t
SELECT g, repeat('x', 400) FROM generate_series(1, 20000) AS g;
DELETE FROM map_super_t WHERE id % 10 = 0;
});
$node->safe_psql(
	'postgres',
	'VACUUM (FREEZE, ANALYZE, TRUNCATE FALSE) map_super_t;');
$node->safe_psql('postgres', 'CHECKPOINT;');

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('map_super_t'::regclass);});
my $map_path = "${main_path}_map";

my $super_first = read_map_super($node, $map_path);
check_format($super_first, $block_size, 'first checkpoint');
my $blocks_first = check_identity_frontiers(
	$node, $super_first, $main_path, $block_size, 'first checkpoint');

$node->safe_psql(
	'postgres', q{
INSERT INTO map_super_t
SELECT g, repeat('y', 400) FROM generate_series(20001, 40000) AS g;
});
$node->safe_psql(
	'postgres',
	'VACUUM (FREEZE, ANALYZE, TRUNCATE FALSE) map_super_t;');
$node->safe_psql('postgres', 'CHECKPOINT;');

my $super_second = read_map_super($node, $map_path);
check_format($super_second, $block_size, 'second checkpoint');
my $blocks_second = check_identity_frontiers(
	$node, $super_second, $main_path, $block_size, 'second checkpoint');

cmp_ok($blocks_second->{main}, '>', $blocks_first->{main},
	'MAIN identity frontier advances after the second insert');

my $super_second_hex = unpack('H*', $super_second);
$node->stop('immediate');
$node->start;

my $super_restart = read_map_super($node, $map_path);
is(unpack('H*', $super_restart), $super_second_hex,
	'superblock payload and CRC survive immediate restart');
my $blocks_restart = check_identity_frontiers(
	$node, $super_restart, $main_path, $block_size, 'after restart');
is_deeply($blocks_restart, $blocks_second,
	'physical fork sizes survive immediate restart');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*), min(id), max(id) FROM map_super_t;}),
	'38000|1|40000',
	'relation data remains readable after immediate restart');

my $map_file = $node->data_dir . "/$map_path";
$node->stop('fast');

open(my $map_fh, '+<:raw', $map_file)
  or BAIL_OUT("could not open \"$map_file\": $!");
my $position = sysseek($map_fh, MAP_CORRUPTION_OFFSET, SEEK_SET);
BAIL_OUT('could not seek to the Umbra MAP reserved payload')
  unless defined($position) && $position == MAP_CORRUPTION_OFFSET;

my $payload_byte;
my $nread = sysread($map_fh, $payload_byte, 1);
BAIL_OUT('could not read the Umbra MAP reserved payload')
  unless defined($nread) && $nread == 1;

$position = sysseek($map_fh, MAP_CORRUPTION_OFFSET, SEEK_SET);
BAIL_OUT('could not seek back to the Umbra MAP reserved payload')
  unless defined($position) && $position == MAP_CORRUPTION_OFFSET;
my $corrupted_byte = chr(ord($payload_byte) ^ 1);
my $nwritten = syswrite($map_fh, $corrupted_byte, 1);
BAIL_OUT('could not corrupt the Umbra MAP reserved payload')
  unless defined($nwritten) && $nwritten == 1;

$position = sysseek($map_fh, MAP_CORRUPTION_OFFSET, SEEK_SET);
BAIL_OUT('could not seek to verify Umbra MAP corruption')
  unless defined($position) && $position == MAP_CORRUPTION_OFFSET;
my $readback;
$nread = sysread($map_fh, $readback, 1);
BAIL_OUT('could not verify Umbra MAP corruption')
  unless defined($nread) && $nread == 1;
is($readback, $corrupted_byte,
	'covered superblock payload byte is corrupted on disk');
close($map_fh) or BAIL_OUT("could not close \"$map_file\": $!");

$node->start;
my ($result, $stdout, $stderr) =
  $node->psql('postgres', q{EXPLAIN SELECT * FROM map_super_t;});

is($result, 3, 'invalid MAP CRC rejects a planner size lookup');
like($stderr, qr/Umbra map superblock is corrupted/,
	'covered-payload corruption is detected by the CRC');

$node->stop;
done_testing();
