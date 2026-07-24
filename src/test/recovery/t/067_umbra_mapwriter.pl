# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that the map writer flushes dirty selector pages without a checkpoint.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub read_active_slot
{
	my ($path, $block_size, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $page_index = int($logical_block / $entries_per_page);
	my $entry_index = $logical_block % $entries_per_page;
	my $group = int($page_index / 256);
	my $map_block = 1 + $group * 258 + 2 + ($page_index % 256);
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

my $node = PostgreSQL::Test::Cluster->new('umbra_mapwriter');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
mapwriter_delay = '20ms'
mapwriter_lru_maxpages = 0
max_wal_size = '4GB'
]);
$node->start;

ok($node->poll_query_until(
		'postgres',
		q[SELECT count(*) = 1
FROM pg_stat_activity WHERE backend_type = 'map writer'],
		't'),
	'map writer is visible as an independent background worker');

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_mapwriter_t(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
]);
my $target_block = 0 + $node->safe_psql(
	'postgres', q[
INSERT INTO umbra_mapwriter_t VALUES (1, 'initial')
RETURNING (ctid::text::point)[0]::integer;
]);
my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres', q[
SELECT pg_relation_filepath('umbra_mapwriter_t'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";

# Set the initial tuple's commit hint before the FPI interval begins, so the
# following UPDATE owns the ordinary FPI and its selector shift.
$node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer FROM umbra_mapwriter_t WHERE id = 1;]);
$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'checkpoint seeds the selector at slot 0');

my $updated_block = 0 + $node->safe_psql(
	'postgres', q[
UPDATE umbra_mapwriter_t SET payload = 'shifted' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
]);
is($updated_block, $target_block, 'update remains on the target page');
is(read_active_slot($map_path, $block_size, $target_block), 0,
	'disabled map writer leaves the dirty selector in memory');

$node->safe_psql(
	'postgres', q[ALTER SYSTEM SET mapwriter_lru_maxpages = 1]);
$node->safe_psql('postgres', q[SELECT pg_reload_conf()]);

my $flushed = 0;
for (1 .. 200)
{
	if (read_active_slot($map_path, $block_size, $target_block) == 1)
	{
		$flushed = 1;
		last;
	}
	usleep(50_000);
}
ok($flushed,
	'map writer flushes the selector page without an explicit checkpoint');

$node->stop('immediate');
$node->start;
is(read_active_slot($map_path, $block_size, $target_block), 1,
	'map writer selector survives crash recovery');
is($node->safe_psql(
		'postgres', q[
SELECT payload FROM umbra_mapwriter_t WHERE id = 1;]),
	'shifted', 'crash recovery follows the map writer selector');

$node->stop;

done_testing();
