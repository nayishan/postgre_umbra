# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the Umbra MAP superblock format, remapped frontiers, and CRC.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_MAGIC => 0x554D4252;
use constant MAP_VERSION => 2;
use constant MAP_FORMAT_V2 => 0x00000002;
use constant MAP_LOGICAL_MAIN_OFFSET => 16;
use constant MAP_LOGICAL_FSM_OFFSET => 20;
use constant MAP_LOGICAL_VM_OFFSET => 24;
use constant MAP_PHYSICAL_MAIN_OFFSET => 28;
use constant MAP_PHYSICAL_FSM_OFFSET => 32;
use constant MAP_PHYSICAL_VM_OFFSET => 36;
use constant MAP_GENERATION_LSN_OFFSET => 40;
use constant MAP_CAPACITY_MAIN_OFFSET => 48;
use constant MAP_CAPACITY_FSM_OFFSET => 52;
use constant MAP_CAPACITY_VM_OFFSET => 56;
use constant MAP_CRC_OFFSET => 60;
use constant MAP_PAYLOAD_SIZE => 64;
use constant MAP_SECTOR_SIZE => 512;

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

sub u64_at
{
	my ($buffer, $offset) = @_;
	return unpack('Q', substr($buffer, $offset, 8));
}

sub read_map_super
{
	my ($node, $map_path) = @_;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($map_path)
		  . ", 0, " . MAP_SECTOR_SIZE . ", false), 'hex');");

	BAIL_OUT('short Umbra MAP superblock')
	  unless length($hex) == 2 * MAP_SECTOR_SIZE;
	return pack('H*', $hex);
}

sub raw_main_blocks
{
	my ($node, $main_path, $block_size) = @_;
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
	is($bytes % $block_size, 0, 'physical MAIN fork is block aligned');
	return int($bytes / $block_size);
}

sub check_root_crc
{
	my ($node, $super, $phase) = @_;
	my $payload_hex = unpack('H*', substr($super, 0, MAP_CRC_OFFSET));
	my $expected = 0 + $node->safe_psql(
		'postgres', "SELECT crc32c(decode('$payload_hex', 'hex'));");

	is(u32_at($super, MAP_CRC_OFFSET), $expected,
		"$phase: superblock CRC covers the root payload");
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
my $payload_repeats = int($block_size / 64);
my $row_count = 1200;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_root_abort_t(id integer, payload text)
  WITH (autovacuum_enabled = false);
CREATE TABLE umbra_root_subabort_t(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_root_abort_t ALTER COLUMN payload SET STORAGE PLAIN;
ALTER TABLE umbra_root_subabort_t ALTER COLUMN payload SET STORAGE PLAIN;
});

my $idle_xact = $node->background_psql('postgres');
$idle_xact->query_safe(q{
BEGIN;
INSERT INTO umbra_root_abort_t VALUES (1, repeat('x', 7000));
});
my ($checkpoint_result, $checkpoint_stdout, $checkpoint_stderr) =
  $node->psql(
	'postgres',
	q{SET statement_timeout = '30s'; CHECKPOINT;});
is($checkpoint_result, 0,
	'published MAP and resident root release the checkpoint delay');
$idle_xact->query_safe('ROLLBACK;');
$idle_xact->quit;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_root_abort_t;'),
	'0', 'top-level abort does not roll back the published storage EOF');
my $abort_main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_root_abort_t'::regclass);});
my $abort_physical_blocks =
  raw_main_blocks($node, $abort_main_path, $block_size);
my $abort_super = read_map_super($node, "${abort_main_path}_map");
cmp_ok($abort_physical_blocks, '>', 0,
	'aborted extension leaves append-only physical storage');
cmp_ok(u32_at($abort_super, MAP_LOGICAL_MAIN_OFFSET), '>', 0,
	'aborted extension remains covered by the authoritative root');

my ($subabort_result, $subabort_stdout, $subabort_stderr) = $node->psql(
	'postgres', q{
BEGIN;
SAVEPOINT umbra_root_savepoint;
INSERT INTO umbra_root_subabort_t VALUES (1, repeat('x', 7000));
ROLLBACK TO umbra_root_savepoint;
COMMIT;
});
is($subabort_result, 0,
	'subtransaction abort keeps the published storage EOF');
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_root_subabort_t;'),
	'0', 'relation remains readable after subtransaction abort');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_super_t(id integer, payload text)
  WITH (autovacuum_enabled = false, vacuum_truncate = true);
ALTER TABLE umbra_super_t ALTER COLUMN payload SET STORAGE PLAIN;
});
$node->safe_psql(
	'postgres', qq{
INSERT INTO umbra_super_t
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, $row_count) AS g;
CHECKPOINT;
});

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_super_t'::regclass);});
my $map_path = "${main_path}_map";
my $identity_blocks = raw_main_blocks($node, $main_path, $block_size);
my $identity_super = read_map_super($node, $map_path);

cmp_ok($identity_blocks, '>', 1000,
	'identity phase is large enough for lazy VACUUM truncation');
is(u32_at($identity_super, 0), MAP_MAGIC, 'superblock magic is UMBR');
is(u32_at($identity_super, 4), MAP_VERSION,
	'superblock version is supported');
is(u32_at($identity_super, 8), $block_size,
	'superblock block size matches the build');
is(u32_at($identity_super, 12), MAP_FORMAT_V2,
	'identity root has only the format flag');
is(u32_at($identity_super, MAP_LOGICAL_MAIN_OFFSET), $identity_blocks,
	'identity logical EOF matches physical EOF');
is(u32_at($identity_super, MAP_PHYSICAL_MAIN_OFFSET), $identity_blocks,
	'identity physical frontier matches physical EOF');
is(u32_at($identity_super, MAP_LOGICAL_FSM_OFFSET),
	u32_at($identity_super, MAP_PHYSICAL_FSM_OFFSET),
	'FSM root fields use the same absent or identity state');
is(u32_at($identity_super, MAP_LOGICAL_VM_OFFSET),
	u32_at($identity_super, MAP_PHYSICAL_VM_OFFSET),
	'VM root fields use the same absent or identity state');
isnt(u64_at($identity_super, MAP_GENERATION_LSN_OFFSET), 0,
	'root identifies the MAIN CREATE WAL generation');
is(u32_at($identity_super, MAP_CAPACITY_MAIN_OFFSET), $identity_blocks,
	'identity MAIN capacity matches physical EOF');
is(u32_at($identity_super, MAP_CAPACITY_FSM_OFFSET),
	u32_at($identity_super, MAP_PHYSICAL_FSM_OFFSET),
	'FSM capacity uses the same absent or identity state');
is(u32_at($identity_super, MAP_CAPACITY_VM_OFFSET),
	u32_at($identity_super, MAP_PHYSICAL_VM_OFFSET),
	'VM capacity uses the same absent or identity state');
is(substr($identity_super, MAP_PAYLOAD_SIZE,
		MAP_SECTOR_SIZE - MAP_PAYLOAD_SIZE),
	"\0" x (MAP_SECTOR_SIZE - MAP_PAYLOAD_SIZE),
	'root occupies one zero-padded 512-byte sector');
check_root_crc($node, $identity_super, 'identity phase');

$node->safe_psql('postgres', 'DELETE FROM umbra_super_t;');
$node->safe_psql(
	'postgres',
	'VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umbra_super_t;');
$node->safe_psql('postgres', 'CHECKPOINT;');

my $truncated_blocks = raw_main_blocks($node, $main_path, $block_size);
my $truncated_super = read_map_super($node, $map_path);

is($truncated_blocks, $identity_blocks,
	'logical truncate retains the append-only physical fork');
is(u32_at($truncated_super, MAP_LOGICAL_MAIN_OFFSET), 0,
	'truncate lowers the root logical EOF to zero');
is(u32_at($truncated_super, MAP_PHYSICAL_MAIN_OFFSET), $identity_blocks,
	'truncate does not rewind the physical frontier');
is(u32_at($truncated_super, MAP_CAPACITY_MAIN_OFFSET), $identity_blocks,
	'truncate does not rewind physical capacity');
check_root_crc($node, $truncated_super, 'truncated phase');

$node->safe_psql(
	'postgres', q{INSERT INTO umbra_super_t VALUES (2000, 'remapped');});
$node->safe_psql('postgres', 'CHECKPOINT;');

my $remapped_blocks = raw_main_blocks($node, $main_path, $block_size);
my $remapped_super = read_map_super($node, $map_path);
my $logical_after = u32_at($remapped_super, MAP_LOGICAL_MAIN_OFFSET);
my $physical_after = u32_at($remapped_super, MAP_PHYSICAL_MAIN_OFFSET);

cmp_ok($remapped_blocks, '>', $identity_blocks,
	're-extension appends beyond retained physical EOF');
cmp_ok($logical_after, '>', 0, 're-extension advances logical EOF');
cmp_ok($logical_after, '<', $physical_after,
	'remapped logical EOF is independent of the physical frontier');
is($physical_after, $remapped_blocks,
	'physical frontier follows append-only physical allocation');
is(u32_at($remapped_super, MAP_CAPACITY_MAIN_OFFSET), $remapped_blocks,
	'physical capacity follows materialized allocation');
is(u32_at($remapped_super, 12), MAP_FORMAT_V2,
	'root flags remain limited to the format marker');
is($node->safe_psql('postgres', 'SELECT id FROM umbra_super_t;'),
	'2000', 'remapped relation remains readable');
check_root_crc($node, $remapped_super, 'remapped phase');

my $remapped_super_hex = unpack('H*', $remapped_super);
$node->stop('immediate');
$node->start;

is(unpack('H*', read_map_super($node, $map_path)), $remapped_super_hex,
	'remapped superblock survives immediate restart');
is($node->safe_psql('postgres', 'SELECT id FROM umbra_super_t;'),
	'2000', 'data remains readable after immediate restart');
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_root_abort_t;'),
	'0', 'authoritative root remains readable after restart');

my $map_file = $node->data_dir . "/$map_path";
$node->stop('fast');

open(my $map_fh, '+<:raw', $map_file)
  or BAIL_OUT("could not open \"$map_file\": $!");
my $position = sysseek($map_fh, MAP_CAPACITY_MAIN_OFFSET, SEEK_SET);
BAIL_OUT('could not seek to the CRC-covered root payload')
  unless defined($position) && $position == MAP_CAPACITY_MAIN_OFFSET;
my $payload_byte;
my $nread = sysread($map_fh, $payload_byte, 1);
BAIL_OUT('could not read the CRC-covered root payload')
  unless defined($nread) && $nread == 1;
$position = sysseek($map_fh, MAP_CAPACITY_MAIN_OFFSET, SEEK_SET);
BAIL_OUT('could not seek back to the CRC-covered root payload')
  unless defined($position) && $position == MAP_CAPACITY_MAIN_OFFSET;
my $nwritten = syswrite($map_fh, chr(ord($payload_byte) ^ 1));
BAIL_OUT('could not corrupt the CRC-covered root payload')
  unless defined($nwritten) && $nwritten == 1;
close($map_fh) or BAIL_OUT("could not close \"$map_file\": $!");

$node->start;
my ($result, $stdout, $stderr) =
  $node->psql('postgres', q{EXPLAIN SELECT * FROM umbra_super_t;});
is($result, 3, 'invalid root CRC rejects a normal relation-size lookup');
like($stderr, qr/Umbra map superblock is corrupted/,
	'root payload corruption is detected by the CRC');

$node->stop;
done_testing();
