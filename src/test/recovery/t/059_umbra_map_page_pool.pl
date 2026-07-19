# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify replacement, persistence, and recovery for ordinary identity MAP
# pages below the superblock root.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use constant INVALID_BLOCK_NUMBER => 0xFFFFFFFF;

sub read_hex_bytes
{
	my ($path, $offset, $length) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $length);
	defined($nread) or BAIL_OUT("could not read \"$path\": $!");
	$nread == $length
	  or BAIL_OUT("short read from \"$path\": read $nread of $length bytes");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return unpack('H*', $buffer);
}

sub write_invalid_block
{
	my ($path, $offset) = @_;
	my $value = pack('L', INVALID_BLOCK_NUMBER);

	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nwritten = syswrite($fh, $value);
	defined($nwritten) && $nwritten == length($value)
	  or BAIL_OUT("could not write \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

sub main_entry_offset
{
	my ($block_size, $lblkno) = @_;
	my $entries_per_page = int($block_size / 4);
	my $map_block = 3 + int($lblkno / $entries_per_page);

	return $map_block * $block_size
	  + ($lblkno % $entries_per_page) * 4;
}

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('map_page_pool');
$node->init;
$node->append_conf('postgresql.conf', "shared_buffers = '128kB'");
$node->append_conf('postgresql.conf', 'max_locks_per_transaction = 512');
$node->append_conf('postgresql.conf', 'mapwriter_lru_maxpages = 0');
$node->start;

$node->safe_psql(
	'postgres',
	q{
CREATE TABLE umbra_pool_anchor (id integer) WITH (autovacuum_enabled = false);
DO $$
BEGIN
	FOR i IN 1..128 LOOP
		EXECUTE format(
			'CREATE TABLE umbra_pool_churn_%s (id integer) WITH (autovacuum_enabled = false)',
			i);
	END LOOP;
END
$$;
});

my $paths = $node->safe_psql(
	'postgres',
	q{
SELECT pg_relation_filepath('umbra_pool_anchor'::regclass),
       pg_relation_filepath('umbra_pool_churn_128'::regclass);
});
my ($anchor_path, $last_path) = split(/\|/, $paths);
my $anchor_map = $node->data_dir . "/${anchor_path}_map";
my $last_map = $node->data_dir . "/${last_path}_map";
my $block_size = $node->safe_psql(
	'postgres',
	q{SELECT setting::int FROM pg_settings WHERE name = 'block_size'});

# The root precedes the [FSM][VM][8192 MAIN] groups.  MAIN entry 0 is
# therefore the first uint32 in map block 3.
my $main_entry_zero_offset = 3 * $block_size;

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', 'INSERT INTO umbra_pool_anchor VALUES (1)');

is(read_hex_bytes($anchor_map, $block_size, 4),
	'ffffffff', 'FSM gap page is initialized to 0xFF');
is(read_hex_bytes($anchor_map, 2 * $block_size, 4),
	'ffffffff', 'VM gap page is initialized to 0xFF');
is(read_hex_bytes($anchor_map, $main_entry_zero_offset, 4),
	'ffffffff', 'new target MAP page remains unchanged on disk while dirty');

# The minimum pool has 16 slots.  Churning many relations ages the anchor
# page and forces a dirty victim writeback.
$node->safe_psql(
	'postgres',
	q{
DO $$
BEGIN
	FOR i IN 1..128 LOOP
		EXECUTE format('INSERT INTO umbra_pool_churn_%s VALUES (1)', i);
	END LOOP;
END
$$;
});

is(read_hex_bytes($anchor_map, $main_entry_zero_offset, 4),
	'00000000', 'clock eviction writes the victim through its own relation tag');
is(read_hex_bytes($last_map, $main_entry_zero_offset, 4),
	'ffffffff', 'most recently used MAP page remains dirty in the pool');

$node->safe_psql('postgres', 'CHECKPOINT');
is(read_hex_bytes($last_map, $main_entry_zero_offset, 4),
	'00000000', 'checkpoint flushes the remaining dirty MAP page');

# INSERT+INIT uses RBM_ZERO_AND_LOCK during redo.  Model a crash that lost one
# first-born entry; the page's WAL record must reinstall its exact mapping
# before initializing the data page.
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_pool_redo_init (id integer, payload text)
  WITH (autovacuum_enabled = false);
CHECKPOINT;
INSERT INTO umbra_pool_redo_init
SELECT g, repeat('z', 400) FROM generate_series(1, 2000) AS g;
});
my $redo_init_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_pool_redo_init'::regclass);});
my $redo_init_map = $node->data_dir . "/${redo_init_path}_map";
my $redo_init_nblocks = 0 + $node->safe_psql(
	'postgres',
	"SELECT pg_relation_size('umbra_pool_redo_init') / $block_size;");
cmp_ok($redo_init_nblocks, '>', 1,
	'post-checkpoint insert created new heap pages');
my $redo_init_block = $redo_init_nblocks - 1;
my $redo_init_offset = main_entry_offset($block_size, $redo_init_block);

$node->stop('immediate');
write_invalid_block($redo_init_map, $redo_init_offset);
is(read_hex_bytes($redo_init_map, $redo_init_offset, 4),
	'ffffffff', 'crash image has a missing first-born MAP entry');

$node->start;
is(
	$node->safe_psql('postgres', 'SELECT count(*) FROM umbra_pool_redo_init'),
	'2000',
	'redo restores an INSERT+INIT page with its WAL-recorded mapping');
$node->safe_psql('postgres', 'CHECKPOINT');
is(
	read_hex_bytes($redo_init_map, $redo_init_offset, 4),
	unpack('H*', pack('L', $redo_init_block)),
	'checkpoint persists the first-born mapping replayed from WAL');
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_pool_churn_128'),
	'1', 'checkpointed relation remains readable after crash restart');

$node->stop;
done_testing();
