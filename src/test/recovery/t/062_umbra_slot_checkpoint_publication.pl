# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify a checkpoint cannot begin between slot-shift WAL insertion and
# selector publication.

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

my $node = PostgreSQL::Test::Cluster->new('umbra_slot_checkpoint_publication');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
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
CREATE TABLE umbra_slot_checkpoint_publication(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_slot_checkpoint_publication VALUES (1, 'initial');
]);
my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_slot_checkpoint_publication'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM umbra_slot_checkpoint_publication WHERE id = 1;]);

# Avoid consuming the first post-checkpoint FPI with a visibility hint read.
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach(
         'umbra-mapping-after-wal-before-publish', 'wait')]);

# INJECTION_POINT_CACHED is deliberately used in the WAL path.  Populate this
# backend's injection cache before it reaches the critical publication region.
my $update = $node->background_psql('postgres');
$update->query_until(
	qr/preload_started/,
	q(\echo preload_started
SELECT injection_points_run(
  'umbra-mapping-after-wal-before-publish');
\echo preload_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$update->query_until(qr/preload_done/, '');

$update->query_until(
	qr/update_started/,
	q(\echo update_started
UPDATE umbra_slot_checkpoint_publication SET payload = 'shifted' WHERE id = 1;
\echo update_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
pass('slot-shift WAL insertion pauses before selector publication');

my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('checkpoint waits for selector publication');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$update->query_until(qr/update_done/, '');
$checkpoint->query_until(qr/checkpoint_done/, '');

is($node->safe_psql('postgres',
		q[SELECT payload FROM umbra_slot_checkpoint_publication WHERE id = 1;]),
	'shifted', 'update and checkpoint complete after selector publication');
is(read_active_slot($map_path, $block_size, $target_block), 1,
	'checkpoint persists the selector target after publication');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach(
         'umbra-mapping-after-wal-before-publish')]);
$update->quit;
$checkpoint->quit;
$node->stop;

done_testing();
