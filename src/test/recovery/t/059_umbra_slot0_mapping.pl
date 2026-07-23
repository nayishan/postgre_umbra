# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify MAIN slot-0 formula mapping, durable capacity, and recovery reads.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant CHUNK_PAGES => 32;
use constant CHUNK_SLOTS => 3;
use constant ROOT_FLAG_MAIN_SLOT0 => 0x00000003;
use constant ROOT_FLAGS_OFFSET => 16;
use constant ROOT_LOGICAL_EOF_OFFSET => 20;
use constant ROOT_RESERVED_OFFSET => 32;
use constant ROOT_PHYSICAL_CAPACITY_OFFSET => 40;
use constant ROOT_TRAILING_RESERVED_OFFSET => 52;

sub expected_capacity
{
	my $logical_eof = shift;

	return 0 if $logical_eof == 0;
	return int(($logical_eof + CHUNK_PAGES - 1) / CHUNK_PAGES)
	  * CHUNK_PAGES * CHUNK_SLOTS;
}

sub relation_blocks
{
	my ($node, $relation) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('$relation'::regclass) / "
		  . "current_setting('block_size')::integer");
}

sub check_layout
{
	my ($node, $relation, $main_path, $block_size, $label) = @_;
	my $logical_eof = relation_blocks($node, $relation);
	my $physical_capacity = expected_capacity($logical_eof);
	my $root = slurp_file($node->data_dir . "/${main_path}_map");
	my $root_flags = unpack('L', substr($root, ROOT_FLAGS_OFFSET, 4));
	my $root_logical_eof =
	  unpack('L', substr($root, ROOT_LOGICAL_EOF_OFFSET, 4));
	my $root_physical_capacity =
	  unpack('L', substr($root, ROOT_PHYSICAL_CAPACITY_OFFSET, 4));
	my $physical_bytes = -s $node->data_dir . "/$main_path";

	is($root_flags & ROOT_FLAG_MAIN_SLOT0, ROOT_FLAG_MAIN_SLOT0,
		"$label: root activates MAIN slot-0 mapping");
	is($root_logical_eof, $logical_eof,
		"$label: root stores the exact logical EOF");
	is(substr($root, ROOT_RESERVED_OFFSET, 8), "\0" x 8,
		"$label: root keeps the reserved generation bytes zero");
	is($root_physical_capacity, $physical_capacity,
		"$label: root stores chunk-aligned physical capacity");
	is(substr($root, ROOT_TRAILING_RESERVED_OFFSET, 8), "\0" x 8,
		"$label: root keeps trailing reserved bytes zero");
	is($physical_bytes, $physical_capacity * $block_size,
		"$label: durable MAIN file has the published physical capacity");

	return $logical_eof;
}

my $node = PostgreSQL::Test::Cluster->new('slot0_mapping');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_method=worker
io_worker_idle_timeout=0ms
io_worker_launch_interval=0ms
io_min_workers=1
checkpoint_timeout=1h
max_wal_size=4GB
));
$node->start;

is($node->safe_psql('postgres', 'SHOW io_method'), 'worker',
	'worker AIO is enabled');

SKIP:
{
	skip 'injection points are unavailable in this build', 1
	  unless defined $ENV{enable_injection_points}
	  && $ENV{enable_injection_points} eq 'yes'
	  && $node->check_extension('injection_points');

	$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
	$node->safe_psql(
		'postgres',
		q{SELECT injection_points_attach('umbra-create-after-wal-before-finish', 'wait')});

	my $create = $node->background_psql('postgres');
	$create->query_until(
		qr/creating_slot0_barrier/,
		q(\echo creating_slot0_barrier
CREATE TABLE umbra_slot0_barrier (id integer);
));
	$node->wait_for_event('client backend',
		'umbra-create-after-wal-before-finish');

	my $checkpoint = $node->background_psql('postgres');
	$checkpoint->query_until(
		qr/starting_slot0_checkpoint/,
		q(\echo starting_slot0_checkpoint
CHECKPOINT;
));
	$node->wait_for_event('checkpointer', 'CheckpointDelayStart');

	$node->safe_psql(
		'postgres',
		q{SELECT injection_points_wakeup('umbra-create-after-wal-before-finish')});
	$create->query_safe(q{SELECT 1});
	$checkpoint->query_safe(q{SELECT 1});
	$create->quit;
	$checkpoint->quit;
	$node->safe_psql(
		'postgres',
		q{SELECT injection_points_detach('umbra-create-after-wal-before-finish')});
	is($node->safe_psql(
			'postgres',
			q{SELECT to_regclass('public.umbra_slot0_barrier')}),
		'umbra_slot0_barrier',
		'CREATE waits out the WAL-to-root activation checkpoint barrier');
}

$node->safe_psql(
	'postgres', q{
CHECKPOINT;
CREATE TABLE umbra_slot0 (id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_slot0
SELECT g, repeat('x', 700)
FROM generate_series(1, 600) AS g;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_slot0'::regclass);});

# Force CREATE and subsequent page redo to reconstruct a missing mapped fork.
$node->stop('immediate');
unlink $node->data_dir . "/$main_path"
  or die "could not remove mapped MAIN file: $!";
unlink $node->data_dir . "/${main_path}_map"
  or die "could not remove mapped root file: $!";
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('x', 700)}),
	'600', 'CREATE and page redo rebuild mapped blocks across a chunk boundary');

my $initial_blocks =
	check_layout($node, 'umbra_slot0', $main_path, $block_size, 'initial grow');
cmp_ok($initial_blocks, '>', CHUNK_PAGES,
	'initial grow crosses the first slot-0 chunk boundary');

# A global SMGRRELEASE closes descriptors but retains SMgrRelation objects.
# Leave a pending-unlink gate in an otherwise empty tablespace so that DROP
# TABLESPACE must issue that barrier.  Open this relation while its root is
# hidden so the retained handle first selects direct I/O, then restore the root
# before the barrier.  The next size lookup must rediscover slot-0 policy
# instead of reporting the three-slot physical capacity.
$node->safe_psql(
	'postgres', q{
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE umbra_policy_ts LOCATION '';
CREATE TABLE umbra_policy_gate (id integer) TABLESPACE umbra_policy_ts;
DROP TABLE umbra_policy_gate;
});
my $map_file = $node->data_dir . "/${main_path}_map";
my $hidden_map_file = "$map_file.policy_hidden";
rename($map_file, $hidden_map_file)
  or BAIL_OUT("could not hide mapped root \"$map_file\": $!");
my $policy_session = $node->background_psql('postgres');
my $direct_blocks = 0 + $policy_session->query_safe(q{
SELECT pg_relation_size('umbra_slot0'::regclass) /
       current_setting('block_size')::integer;
});
is($direct_blocks, expected_capacity($initial_blocks),
	'retained handle initially observes the direct physical size');
rename($hidden_map_file, $map_file)
  or BAIL_OUT("could not restore mapped root \"$map_file\": $!");
$node->safe_psql('postgres', 'DROP TABLESPACE umbra_policy_ts');
is(0 + $policy_session->query_safe(q{
SELECT pg_relation_size('umbra_slot0'::regclass) /
       current_setting('block_size')::integer;
}), $initial_blocks,
	'retained handle refreshes MAIN slot-0 policy after SMGRRELEASE');
$policy_session->quit;

$node->stop;
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('x', 700)}),
	'600', 'worker AIO reads all mapped pages after restart');

$node->safe_psql(
	'postgres', q{
UPDATE umbra_slot0
SET payload = repeat('u', 700)
WHERE id IN (1, 32, 33, 600);
CHECKPOINT;
});
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('u', 700)}),
	'4', 'updates write mapped pages on both sides of the chunk boundary');
check_layout($node, 'umbra_slot0', $main_path, $block_size, 'after update');

# Exercise checkpoint root writeback followed by crash recovery.
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('u', 700)}),
	'4', 'checkpointed root and its mapped MAIN pages survive a crash');
check_layout($node, 'umbra_slot0', $main_path, $block_size,
	'after checkpoint recovery');

is($node->safe_psql(
		'postgres', q{
WITH relation_parts AS
(
  SELECT oid, reltoastrelid
  FROM pg_class
  WHERE oid = 'umbra_slot0'::regclass
)
SELECT pg_table_size(oid) - pg_relation_size(oid) =
       pg_relation_size(oid, 'fsm') + pg_relation_size(oid, 'vm') +
       CASE WHEN reltoastrelid <> 0
            THEN pg_total_relation_size(reltoastrelid)
            ELSE 0
       END
FROM relation_parts
}), 't', 'aggregate relation size uses the logical MAIN EOF');

my $main_file = $node->data_dir . "/$main_path";
my $pretruncate_bytes = -s $main_file;

$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_slot0;
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_slot0;
});
my $truncated_blocks =
	check_layout($node, 'umbra_slot0', $main_path, $block_size, 'after truncate');
cmp_ok($truncated_blocks, '<', $initial_blocks,
	'VACUUM truncates the logical EOF');
cmp_ok($pretruncate_bytes, '>', expected_capacity($truncated_blocks) * $block_size,
	'truncate crosses a physical capacity boundary');

# Recreate the persisted-root/raw-tail crash window before truncate redo.
$node->stop('immediate');
truncate($main_file, $pretruncate_bytes)
  or die "could not restore stale mapped MAIN tail: $!";
$node->start;
check_layout($node, 'umbra_slot0', $main_path, $block_size,
	'after truncate redo cleans a stale physical tail');

$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_slot0
SELECT g, repeat('r', 700)
FROM generate_series(1, 600) AS g;
CHECKPOINT;
});
my $regrown_blocks =
	check_layout($node, 'umbra_slot0', $main_path, $block_size, 'after regrow');
cmp_ok($regrown_blocks, '>', CHUNK_PAGES,
	'regrow crosses the first slot-0 chunk boundary again');

$node->stop;
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('r', 700)}),
	'600', 'regrown mapped pages survive restart');

$node->stop;

my $minimal = PostgreSQL::Test::Cluster->new('slot0_minimal');
$minimal->init;
$minimal->append_conf(
	'postgresql.conf', qq(
wal_level=minimal
wal_skip_threshold=1GB
checkpoint_timeout=1h
max_wal_size=4GB
));
$minimal->start;

is($minimal->safe_psql('postgres', 'SHOW wal_level'), 'minimal',
	'pending-sync coverage runs with minimal WAL');
$minimal->safe_psql(
	'postgres', q{
BEGIN;
CREATE TABLE umbra_slot0_minimal (id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_slot0_minimal
SELECT g, repeat('m', 700)
FROM generate_series(1, 600) AS g;
COMMIT;
});

my $minimal_block_size =
	0 + $minimal->safe_psql('postgres', 'SHOW block_size');
my $minimal_path = $minimal->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_slot0_minimal'::regclass);});
my $minimal_blocks = check_layout($minimal, 'umbra_slot0_minimal',
	$minimal_path, $minimal_block_size, 'minimal-WAL commit');
cmp_ok($minimal_blocks, '>', CHUNK_PAGES,
	'minimal-WAL relation crosses the first slot-0 chunk boundary');

$minimal->stop('immediate');
$minimal->start;
is($minimal->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0_minimal WHERE payload = repeat('m', 700)}),
	'600', 'minimal-WAL pending sync preserves mapped data across a crash');
check_layout($minimal, 'umbra_slot0_minimal', $minimal_path,
	$minimal_block_size, 'minimal-WAL restart');

$minimal->stop;
done_testing();
