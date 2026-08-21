# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify target-only active-slot shifts, their WAL header, and redo ordering.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $page_index = int($logical_block / $entries_per_page);
	my $entry_index = $logical_block % $entries_per_page;
	my $group = int($page_index / 256);
	my $map_block = $group * 258 + 2 + ($page_index % 256);
	my $offset = $map_block * $block_size + int($entry_index / 4);
	my $byte;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $byte, 1);
	defined($nread) && $nread == 1
	  or BAIL_OUT("could not read selector from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return (unpack('C', $byte) >> (($entry_index % 4) * 2)) & 0x03;
}

sub assert_shift_wal
{
	my ($node, $filenode, $target_block, $start_lsn, $end_lsn,
		$source_slot, $target_slot, $label) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	# --start can land between WAL records; pg_waldump reports that single
	# skipped range on stderr before decoding the next complete record.
	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', "$label: pg_waldump reads the update WAL");
	my @records = grep {
		/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
	} split(/\n/, $dump);
	my @selector_shifts = grep {
		/slot shift: source_slot $source_slot target_slot $target_slot/
	} @records;
	is(scalar(@selector_shifts), 1,
		"$label: WAL records one target-only slot transition");
	unlike($selector_shifts[0] // '', qr/\bFPW\b/,
		"$label: normal shift omits the full-page image");
}

my $node = PostgreSQL::Test::Cluster->new('umbra_slot_shift_wal');
# Keep this test focused on ordinary target-only shifts.  HINT_DELTA has
# dedicated coverage in 069, so disable both reasons for logging hints here.
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
wal_log_hints = off
max_wal_size = '4GB'
log_min_messages = debug1
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_slot_shift_wal(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_slot_shift_wal VALUES (1, 'initial');
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_slot_shift_wal'::regclass);]);
my $filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_slot_shift_wal'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_shift_wal WHERE id = 1;]);

# The lookup above may set the tuple's commit hint.  Persist it before the
# first shift interval so each following UPDATE is the operation under test.
$node->safe_psql('postgres', 'CHECKPOINT');

is(-s $map_path, 3 * $block_size,
	'initial insert and checkpoint seed the first selector group');
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'initial selector is slot 0');

my @steps = (
	[0, 1, 'one'],
	[1, 2, 'two'],
	[2, 0, 'three'],
);

for my $step_index (0 .. $#steps)
{
	my ($source_slot, $target_slot, $payload) = @{ $steps[$step_index] };
	my $label = "slot $source_slot to $target_slot";
	my $start_lsn = $node->safe_psql(
		'postgres', 'SELECT pg_current_wal_insert_lsn();');
	my $updated_block = 0 + $node->safe_psql(
		'postgres',
		"UPDATE umbra_slot_shift_wal SET payload = '$payload' "
		  . "WHERE id = 1 RETURNING (ctid::text::point)[0]::integer;");
	my $end_lsn = $node->safe_psql(
		'postgres', 'SELECT pg_current_wal_insert_lsn();');

	is($updated_block, $target_block, "$label: update remains on one page");
	assert_shift_wal($node, $filenode, $target_block, $start_lsn, $end_lsn,
		$source_slot, $target_slot, $label);
	# Each following update starts in a fresh slot-shift interval.
	if ($step_index < $#steps)
	{
		$node->safe_psql('postgres', 'CHECKPOINT');
		is(read_active_slot($map_path, $block_size, $target_block),
			$target_slot, "$label: checkpoint persists the selector target");

		# A fresh backend may set the prior update's commit hint when it next
		# reads this tuple.  Persist that hint before the next checkpoint
		# interval; hint WAL is disabled here, so the selector does not move.
		$node->safe_psql('postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_shift_wal WHERE id = 1;]);
		$node->safe_psql('postgres', 'CHECKPOINT');
	}
}

# The final shift is intentionally not checkpointed.  Recovery must publish
# its selector target after materializing the preceding source slot.
$node->stop('immediate');
$node->start;
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'crash redo persists the final selector target');
is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_slot_shift_wal WHERE id = 1;]),
	'three', 'crash redo restores the final update through the target slot');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres',
	q[UPDATE umbra_slot_shift_wal SET payload = 'four' WHERE id = 1;]);
is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_slot_shift_wal WHERE id = 1;]),
	'four', 'a post-recovery update uses the selector target as its source');
$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $target_block), 1,
	'a post-recovery shift advances the old logical page to slot 1');

# DELETE plus VACUUM (TRUNCATE) shrinks the existing fork without assigning a
# new relfilenode.  Regrowth must not inherit the old selector target.
$node->safe_psql(
	'postgres', q[
DELETE FROM umbra_slot_shift_wal;
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_slot_shift_wal;
INSERT INTO umbra_slot_shift_wal VALUES (2, 'regrown');
CHECKPOINT;
]);
my $regrown_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_shift_wal WHERE id = 2;]);
is($regrown_block, $target_block,
	'regrowth reuses the truncated logical block');
is(read_active_slot($map_path, $block_size, $regrown_block), 0,
	'regrowth restores the selector default before the physical triple returns');

# A later DROP can remove the selector fork before a crash even though the
# checkpoint redo point still precedes the shift.  Redo must tolerate a
# missing or zero-length selector fork until that later DROP is replayed.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_slot_shift_drop(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_slot_shift_drop VALUES (1, 'initial');
CREATE TABLE umbra_slot_shift_zero_map(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_slot_shift_zero_map VALUES (1, 'initial');
]);
my $drop_relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_slot_shift_drop'::regclass);]);
my $drop_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_slot_shift_drop'::regclass);]);
my $drop_main_path = $node->data_dir . "/$drop_relpath";
my $drop_map_path = $node->data_dir . "/${drop_relpath}_map";
my $drop_target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_shift_drop WHERE id = 1;]);
my $zero_relpath = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filepath('umbra_slot_shift_zero_map'::regclass);]);
my $zero_filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umbra_slot_shift_zero_map'::regclass);]);
my $zero_main_path = $node->data_dir . "/$zero_relpath";
my $zero_map_path = $node->data_dir . "/${zero_relpath}_map";
my $zero_target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_shift_zero_map WHERE id = 1;]);
$node->safe_psql('postgres', 'CHECKPOINT');

my $drop_start_lsn = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q[
UPDATE umbra_slot_shift_drop SET payload = 'shifted' WHERE id = 1;]);
my $drop_end_lsn = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
assert_shift_wal($node, $drop_filenode, $drop_target_block,
	$drop_start_lsn, $drop_end_lsn, 0, 1, 'shift before later drop');

my $zero_start_lsn = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q[
UPDATE umbra_slot_shift_zero_map SET payload = 'shifted' WHERE id = 1;]);
my $zero_end_lsn = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
assert_shift_wal($node, $zero_filenode, $zero_target_block,
	$zero_start_lsn, $zero_end_lsn, 0, 1,
	'shift before later drop with interrupted unlink');

$node->safe_psql(
	'postgres',
	'DROP TABLE umbra_slot_shift_drop, umbra_slot_shift_zero_map');
ok(!-e $drop_map_path, 'later drop removes the selector mapping before crash');
ok(!-e $zero_map_path,
	'later drop removes the second selector mapping before crash');
$node->stop('immediate');

# Redo DROP removes MAIN immediately.  Delete both delayed MAIN files to model
# a crash after that record was replayed but before recovery completed.
unlink($drop_main_path)
  or BAIL_OUT("could not remove \"$drop_main_path\": $!");
unlink($zero_main_path)
  or BAIL_OUT("could not remove \"$zero_main_path\": $!");
ok(!-e $drop_main_path && !-e $zero_main_path,
	'repeated recovery starts with both dropped MAIN files absent');

# A crash during metadata unlink can instead leave an existing but zero-length
# selector fork.
open(my $zero_map_fh, '>', $zero_map_path)
  or BAIL_OUT("could not create \"$zero_map_path\": $!");
close($zero_map_fh)
  or BAIL_OUT("could not close \"$zero_map_path\": $!");
ok(-z $zero_map_path,
	'interrupted drop leaves a zero-length selector mapping before crash');

my $recovery_log_offset = -s $node->logfile;
$node->start;
ok($node->log_contains(
		qr/page $drop_target_block of relation .*\/$drop_filenode does not exist/,
		$recovery_log_offset),
	'missing source records the target-only shift as an invalid-page dependency');
ok($node->log_contains(
		qr/page $zero_target_block of relation .*\/$zero_filenode does not exist/,
		$recovery_log_offset),
	'zero-length source records the target-only shift as an invalid-page dependency');
is($node->safe_psql(
		'postgres', q[SELECT to_regclass('umbra_slot_shift_drop') IS NULL;]),
	't', 'later DROP keeps the missing-selector relation absent');
is($node->safe_psql(
		'postgres', q[SELECT to_regclass('umbra_slot_shift_zero_map') IS NULL;]),
	't', 'later DROP keeps the zero-length-selector relation absent');

$node->stop;

done_testing();
