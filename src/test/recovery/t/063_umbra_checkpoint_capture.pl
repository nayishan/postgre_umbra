# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a checkpoint recaptures slot-shift publication after its first
# delay scan and before it selects buffers.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $entry_index = $logical_block % $entries_per_page;
	my $map_block = 1 + int($logical_block / $entries_per_page);
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

sub assert_fpi_shift_wal
{
	my ($node, $filenode, $target_block, $start_lsn, $end_lsn) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', 'pg_waldump reads the pre-selection update WAL');
	my @records = grep {
		/rel \d+\/\d+\/$filenode fork main blk $target_block\b/
	} split(/\n/, $dump);
	is(scalar(@records), 1,
		'pre-selection update emits one target-block WAL record');
	like($records[0],
		qr/\bFPW\b.*slot shift: source_slot 0 target_slot 1/,
		'capture-before-selection shift retains an FPI');
	unlike($records[0], qr/captured_source/,
		'capture-before-selection shift does not claim a captured source');
}

my $node = PostgreSQL::Test::Cluster->new('umbra_checkpoint_capture');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
mapwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
max_wal_size = '4GB'
]);
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_checkpoint_capture(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_checkpoint_capture VALUES (1, 'initial');
]);
my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_checkpoint_capture'::regclass);]);
my $filenode = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('umbra_checkpoint_capture'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_checkpoint_capture WHERE id = 1;]);

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach('umbra-checkpoint-after-capture', 'wait')]);
$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach(
         'umbra-mapping-after-wal-before-publish', 'wait')]);

my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-capture');
pass('checkpoint publishes its capture before the fresh delay scan');

# The selector publication point is cached in the writer, so preload it before
# the actual update reaches the critical publication region.
my $writer = $node->background_psql('postgres');
my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$writer->query_until(
	qr/preload_started/,
	q(\echo preload_started
SELECT injection_points_run('umbra-mapping-after-wal-before-publish');
\echo preload_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$writer->query_until(qr/preload_done/, '');

$writer->query_until(
	qr/update_started/,
	q(\echo update_started
UPDATE umbra_checkpoint_capture SET payload = 'shifted' WHERE id = 1;
\echo update_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
pass('slot shift pauses after WAL and before selector publication');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-capture')]);
$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('fresh delay scan catches the post-capture slot shift');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$writer->query_until(qr/update_done/, '');
$checkpoint->query_until(qr/checkpoint_done/, '');
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

assert_fpi_shift_wal($node, $filenode, $target_block, $wal_start, $wal_end);

is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_checkpoint_capture WHERE id = 1;]),
	'shifted', 'update and checkpoint complete after selector publication');
is(read_active_slot($map_path, $block_size, $target_block), 1,
	'checkpoint persists the captured selector target');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-capture')]);
$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach(
         'umbra-mapping-after-wal-before-publish')]);
$writer->quit;
$checkpoint->quit;
$node->stop;

done_testing();
