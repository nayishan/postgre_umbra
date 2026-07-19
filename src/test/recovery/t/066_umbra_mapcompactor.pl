# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that compaction relocates the latest shared-buffer image, that its
# exact remap WAL survives a crash, and that storage lifecycle locks are
# released at the transaction boundaries where compaction may resume.

use strict;
use warnings FATAL => 'all';

use Digest::MD5 qw(md5_hex);
use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_GROUP_TOTAL_PAGES => 8194;

sub read_bytes
{
	my ($path, $offset, $length) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $length);
	defined($nread) && $nread == $length
	  or BAIL_OUT("could not read $length bytes from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
	return $buffer;
}

sub read_main_map_entry
{
	my ($path, $block_size, $lblkno) = @_;
	my $entries_per_page = int($block_size / 4);
	my $fork_page = int($lblkno / $entries_per_page);
	my $group = int($fork_page / MAP_GROUP_MAIN_PAGES);
	my $map_block = 1 + $group * MAP_GROUP_TOTAL_PAGES + 2
	  + ($fork_page % MAP_GROUP_MAIN_PAGES);
	my $offset = $map_block * $block_size
	  + ($lblkno % $entries_per_page) * 4;

	return unpack('L', read_bytes($path, $offset, 4));
}

sub physical_block_is_unwritten
{
	my ($path, $block_size, $pblkno) = @_;
	my $offset = $pblkno * $block_size;
	my $size = -s $path;

	return 1 if !defined($size) || $size < $offset + $block_size;
	return md5_hex(read_bytes($path, $offset, $block_size)) eq
	  md5_hex("\0" x $block_size);
}

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

my $node = PostgreSQL::Test::Cluster->new('umbra_mapcompactor');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
max_prepared_transactions = 10
map_compactor_enable = off
map_compactor_extent_blocks = 128
map_compactor_low_live_percent = 100
map_compactor_max_moves = 1
mapcompactor_delay = '10ms'
mapcompactor_max_relations = 10000
mapwriter_delay = '10ms'
mapwriter_lru_maxpages = 100
});
$node->start;

ok($node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_stat_activity
		  WHERE backend_type = 'map compactor'},
		't'),
	'map compactor is visible as an independent background worker');

my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'))});
my $payload_repeats = int($block_size / 64);
my $payload_bytes = int($block_size / 2);
$node->safe_psql(
	'postgres', qq{
CREATE TABLE umbra_mapcompact_t(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_mapcompact_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_mapcompact_t
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, 300) AS g;
SELECT count(*) FROM umbra_mapcompact_t;
CHECKPOINT;
});

my $relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_mapcompact_t')});
my $filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_mapcompact_t')});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
is(read_main_map_entry($map_path, $block_size, 0), 0,
	'block 0 starts with its identity mapping');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql(
	'postgres', qq{
UPDATE umbra_mapcompact_t
SET payload = repeat('u', $payload_bytes) WHERE id = 1;
});

# The post-checkpoint update first performs the ordinary exact remap.  Wait for
# mapwriter to expose it on disk without writing the dirty data buffer.
my $source_pblkno = 0;
for (1 .. 200)
{
	$source_pblkno = read_main_map_entry($map_path, $block_size, 0);
	last if $source_pblkno != 0;
	usleep(50_000);
}
cmp_ok($source_pblkno, '>', 0,
	'ordinary update remaps block 0 before compaction');
ok(physical_block_is_unwritten(
		$data_path, $block_size, $source_pblkno),
	'ordinary remap source P has not received the dirty shared buffer');

# Advance the physical frontier into a later extent.  The update's source P is
# then eligible for relocation, but block 0 remains resident and dirty.
$node->safe_psql(
	'postgres', qq{
INSERT INTO umbra_mapcompact_t
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(301, 500) AS g;
});
my $state_before = $node->safe_psql(
	'postgres', q{
SELECT count(*)::text || '|' || sum(id)::bigint || '|' ||
       sum(length(payload))::bigint || '|' ||
       (SELECT md5(payload) FROM umbra_mapcompact_t WHERE id = 1)
FROM umbra_mapcompact_t;
});
ok(physical_block_is_unwritten(
		$data_path, $block_size, $source_pblkno),
	'ordinary remap source P is still unwritten when compaction starts');

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
});

my $target_pblkno = $source_pblkno;
for (1 .. 400)
{
	$target_pblkno = read_main_map_entry($map_path, $block_size, 0);
	last if $target_pblkno != $source_pblkno;
	usleep(50_000);
}
cmp_ok($target_pblkno, '!=', $source_pblkno,
	'map compactor publishes a new physical block for block 0');

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = off;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SELECT pg_switch_wal();
});
$target_pblkno = read_main_map_entry($map_path, $block_size, 0);
ok(physical_block_is_unwritten(
		$data_path, $block_size, $target_pblkno),
	'compactor target P is still unwritten before crash recovery');

my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn()');
my ($wal, $wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $wal_start, '--end', $wal_end
	]);
is(normalize_waldump_stderr($wal_stderr), '',
	'pg_waldump reads compactor WAL');
my @relocation_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk 0/ &&
	/; remap: lblkno 0 old_pblkno $source_pblkno new_pblkno $target_pblkno/
} split(/\n/, $wal);
cmp_ok(scalar(@relocation_records), '>=', 1,
	'compactor WAL names the exact source and target physical blocks');
ok(!grep(!/\bFPW\b/, @relocation_records),
	'compactor always keeps a complete page image');
ok(!grep(!/hole: offset: 0, length: 0/, @relocation_records),
	'compactor page image preserves every byte of the shared buffer');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres', q{
SELECT count(*)::text || '|' || sum(id)::bigint || '|' ||
       sum(length(payload))::bigint || '|' ||
       (SELECT md5(payload) FROM umbra_mapcompact_t WHERE id = 1)
FROM umbra_mapcompact_t;
}),
	$state_before,
	'crash recovery restores the latest shared-buffer image at target P');
is(read_main_map_entry($map_path, $block_size, 0), $target_pblkno,
	'crash recovery preserves the compactor mapping');

# Lifecycle operations retain a session-level locator lock until their WAL and
# required physical unlink are complete.  Subabort and PREPARE must release it.
my $lifecycle_log_offset = -s $node->logfile;
my $create_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
CREATE TABLE umbra_storage_lock_t(id integer);
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND mode = 'AccessExclusiveLock'
  AND pid = pg_backend_pid() AND granted;
ROLLBACK;
});
cmp_ok($create_locks, '>=', 1,
	'CREATE holds an exclusive physical storage lifecycle lock');

$node->safe_psql(
	'postgres', q{CREATE TABLE umbra_storage_lock_t(id integer)});
my $drop_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
DROP TABLE umbra_storage_lock_t;
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND mode = 'AccessExclusiveLock'
  AND pid = pg_backend_pid() AND granted;
ROLLBACK;
});
cmp_ok($drop_locks, '>=', 1,
	'DROP holds an exclusive physical storage lifecycle lock');

my $truncate_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
TRUNCATE umbra_storage_lock_t;
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND mode = 'AccessExclusiveLock'
  AND pid = pg_backend_pid() AND granted;
ROLLBACK;
});
cmp_ok($truncate_locks, '>=', 1,
	'TRUNCATE holds an exclusive physical storage lifecycle lock');

my $subabort_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
SAVEPOINT storage_lock_subxact;
CREATE TABLE umbra_storage_lock_subxact(id integer);
ROLLBACK TO storage_lock_subxact;
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND pid = pg_backend_pid() AND granted;
ROLLBACK;
});
is($subabort_locks, 0,
	'subtransaction abort releases its physical storage lifecycle lock');

my $prepared_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
TRUNCATE umbra_storage_lock_t;
DROP TABLE umbra_storage_lock_t;
PREPARE TRANSACTION 'umbra_compactor_drop';
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND pid = pg_backend_pid() AND granted;
});
is(
	$prepared_locks,
	0,
	'PREPARE releases session-level physical storage lifecycle locks');
$node->safe_psql('postgres', "COMMIT PREPARED 'umbra_compactor_drop'");
is($node->safe_psql(
		'postgres', q{SELECT to_regclass('umbra_storage_lock_t') IS NULL}),
	't', 'COMMIT PREPARED completes truncate-and-drop storage cleanup');

$node->safe_psql(
	'postgres', q{
BEGIN;
CREATE TABLE umbra_storage_lock_abort(id integer);
PREPARE TRANSACTION 'umbra_compactor_abort';
});
$node->safe_psql('postgres', "ROLLBACK PREPARED 'umbra_compactor_abort'");
is($node->safe_psql(
		'postgres', q{SELECT to_regclass('umbra_storage_lock_abort') IS NULL}),
	't', 'ROLLBACK PREPARED removes storage created by the prepared transaction');

$node->safe_psql(
	'postgres', q{
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE umbra_compactor_ts LOCATION '';
CREATE TABLE umbra_storage_copy_t(id integer);
INSERT INTO umbra_storage_copy_t VALUES (1), (2);
});
my $copy_prepare_locks = 0 + $node->safe_psql(
	'postgres', q{
BEGIN;
ALTER TABLE umbra_storage_copy_t SET TABLESPACE umbra_compactor_ts;
PREPARE TRANSACTION 'umbra_compactor_copy';
SELECT count(*) FROM pg_locks
WHERE locktype = 'relationstorage' AND pid = pg_backend_pid() AND granted;
});
is($copy_prepare_locks, 0,
	'direct storage copy leaves no mixed-owner lock at PREPARE');
$node->safe_psql('postgres', "ROLLBACK PREPARED 'umbra_compactor_copy'");
is($node->safe_psql(
		'postgres', q{SELECT count(*) FROM umbra_storage_copy_t}),
	'2', 'ROLLBACK PREPARED preserves the source relation after storage copy');
$node->safe_psql(
	'postgres', q{
DROP TABLE umbra_storage_copy_t;
DROP TABLESPACE umbra_compactor_ts;
});
ok(!$node->log_contains(
		qr/you don't own a lock of type AccessExclusiveLock/,
		$lifecycle_log_offset),
	'lifecycle cleanup releases only locks still owned by the backend');

$node->stop;
done_testing();
