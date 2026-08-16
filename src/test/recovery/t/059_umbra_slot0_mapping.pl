# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the exact MAIN three-bucket layout, truncate/regrow reset, and redo.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub relation_blocks
{
	my ($node, $relation) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('$relation'::regclass) / "
		  . "current_setting('block_size')::integer");
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
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	sysread($fh, $byte, 1) == 1
	  or BAIL_OUT("could not read selector from \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return (unpack('C', $byte) >> (($entry_index % 4) * 2)) & 0x03;
}

sub check_main_layout
{
	my ($node, $relation, $main_file, $block_size, $label) = @_;
	my $logical_eof = relation_blocks($node, $relation);
	my $physical_blocks = (-s $main_file) / $block_size;

	is($physical_blocks, 3 * $logical_eof,
		"$label: physical MAIN length is exactly three times logical EOF");
	return $logical_eof;
}

my $node = PostgreSQL::Test::Cluster->new('slot0_three_buckets');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '4GB'
]);
$node->start;

# Leave CREATE and all initial page images after the redo point.
$node->safe_psql(
	'postgres', q{
CHECKPOINT;
CREATE TABLE umbra_slot0 (id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_slot0
SELECT g, repeat('x', 700)
FROM generate_series(1, 600) AS g;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_slot0'::regclass);});
my $main_file = $node->data_dir . "/$main_path";
my $map_path = "${main_file}_map";

# CREATE/page redo needs no relation root: the globally fixed geometry tells
# redo how to reconstruct each page, and an absent selector is slot 0.
$node->stop('immediate');
unlink $main_file or BAIL_OUT("could not remove \"$main_file\": $!");
unlink $map_path or BAIL_OUT("could not remove \"$map_path\": $!");
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('x', 700)}),
	'600', 'CREATE and page redo reconstruct mapped MAIN without a root');

my $initial_blocks = check_main_layout($node, 'umbra_slot0', $main_file,
	$block_size, 'initial recovery');
cmp_ok($initial_blocks, '>', 1,
	'initial relation has more than one logical page');
is(-s $map_path, 3 * $block_size,
	'first selector group has no reserved root block');

my ($tail_id, $tail_block) = split(/\|/, $node->safe_psql(
	'postgres', q{
SELECT id, (ctid::text::point)[0]::integer
FROM umbra_slot0
ORDER BY (ctid::text::point)[0]::integer DESC, id DESC
LIMIT 1;
}));
$node->safe_psql('postgres', 'CHECKPOINT');
is(0 + $node->safe_psql('postgres',
	"UPDATE umbra_slot0 SET payload = repeat('u', 700) "
	  . "WHERE id = $tail_id RETURNING (ctid::text::point)[0]::integer;"),
	$tail_block, 'tail update remains on its existing logical block');
$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_path, $block_size, $tail_block), 1,
	'tail update advances its selector from slot 0 to slot 1');

my $pretruncate_bytes = -s $main_file;
$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_slot0;
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_slot0;
});
my $truncated_blocks = check_main_layout($node, 'umbra_slot0', $main_file,
	$block_size, 'after truncate');
cmp_ok($truncated_blocks, '<', $initial_blocks,
	'truncate lowers logical EOF through physical length');
cmp_ok($tail_block, '>=', $truncated_blocks,
	'the shifted selector belongs to the truncated tail');

# Redo must remove any stale physical tail even though there is no root EOF.
$node->stop('immediate');
truncate($main_file, $pretruncate_bytes)
	  or BAIL_OUT("could not restore stale tail in \"$main_file\": $!");
$node->start;
is(check_main_layout($node, 'umbra_slot0', $main_file, $block_size,
		'after truncate redo'), $truncated_blocks,
	'truncate redo restores the physical EOF');

$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_slot0
SELECT g, repeat('r', 700)
FROM generate_series(1, 600) AS g;
CHECKPOINT;
});
my $regrown_blocks = check_main_layout($node, 'umbra_slot0', $main_file,
	$block_size, 'after regrow');
cmp_ok($regrown_blocks, '>', $tail_block,
	'regrow reaches the former shifted logical block');
is(read_active_slot($map_path, $block_size, $tail_block), 0,
	'regrowth resets the stale truncated-tail selector to slot 0');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0 WHERE payload = repeat('r', 700)}),
	'600', 'regrown slot-0 pages survive crash recovery');
check_main_layout($node, 'umbra_slot0', $main_file, $block_size,
	'after regrow recovery');
$node->stop;

# A WAL-skipping relation still has no separate metadata authority to sync.
my $minimal = PostgreSQL::Test::Cluster->new('slot0_three_buckets_minimal');
$minimal->init(no_data_checksums => 1);
$minimal->append_conf(
	'postgresql.conf', q[
wal_level = minimal
wal_skip_threshold = '1GB'
checkpoint_timeout = '1h'
max_wal_size = '4GB'
]);
$minimal->start;
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
my $minimal_file = $minimal->data_dir . "/$minimal_path";
check_main_layout($minimal, 'umbra_slot0_minimal', $minimal_file,
	$minimal_block_size, 'minimal-WAL commit');
$minimal->stop('immediate');
$minimal->start;
is($minimal->safe_psql('postgres',
		q{SELECT count(*) FROM umbra_slot0_minimal WHERE payload = repeat('m', 700)}),
	'600', 'minimal-WAL pending sync preserves three-bucket data after a crash');
check_main_layout($minimal, 'umbra_slot0_minimal', $minimal_file,
	$minimal_block_size, 'minimal-WAL recovery');
$minimal->stop;

done_testing();
