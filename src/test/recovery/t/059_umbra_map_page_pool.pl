# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify clock eviction and independent persistence for ordinary MAP pages.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub read_hex_bytes
{
	my ($path, $offset, $length) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, 0))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $length);
	defined($nread) or BAIL_OUT("could not read \"$path\": $!");
	$nread == $length
	  or BAIL_OUT("short read from \"$path\": read $nread of $length bytes");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return unpack('H*', $buffer);
}

if (!check_pg_config('#define USE_UMBRA 1'))
{
	plan skip_all => 'Umbra storage manager not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('map_page_pool');
$node->init;
$node->append_conf('postgresql.conf', "shared_buffers = '128kB'");
$node->append_conf('postgresql.conf', 'max_locks_per_transaction = 512');
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
my $main_entry_zero_offset = 3 * $block_size;
my $main_nblocks_offset = 40;

$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', 'INSERT INTO umbra_pool_anchor VALUES (1)');

is(read_hex_bytes($anchor_map, $block_size, 4),
	'ffffffff', 'first MAP gap page is initialized to 0xFF');
is(read_hex_bytes($anchor_map, 2 * $block_size, 4),
	'ffffffff', 'second MAP gap page is initialized to 0xFF');
is(read_hex_bytes($anchor_map, $main_entry_zero_offset, 4),
	'ffffffff', 'new target MAP page remains clean on disk while cached dirty');
is(read_hex_bytes($anchor_map, $main_nblocks_offset, 4),
	'00000000', 'anchor superblock remains dirty in its resident cache');

# The minimum pool has 16 slots.  Eight clock revolutions are enough to age
# even a maximum-usage anchor page and force a dirty victim writeback.
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
	'00000000', 'clock eviction writes the anchor MAP page through its own tag');
is(read_hex_bytes($anchor_map, $main_nblocks_offset, 4),
	'00000000', 'ordinary-page eviction does not flush the anchor superblock');
is(read_hex_bytes($last_map, $main_entry_zero_offset, 4),
	'ffffffff', 'most recently used MAP page remains dirty in the pool');
is(read_hex_bytes($last_map, $main_nblocks_offset, 4),
	'00000000', 'most recently used superblock remains resident and dirty');

$node->safe_psql('postgres', 'CHECKPOINT');
is(read_hex_bytes($last_map, $main_entry_zero_offset, 4),
	'00000000', 'checkpoint flushes the remaining ordinary MAP page');
isnt(read_hex_bytes($anchor_map, $main_nblocks_offset, 4),
	'00000000', 'checkpoint flushes the anchor superblock');
isnt(read_hex_bytes($last_map, $main_nblocks_offset, 4),
	'00000000', 'checkpoint flushes the last superblock');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_pool_anchor'),
	'1', 'evicted relation remains readable after crash restart');
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_pool_churn_128'),
	'1', 'cached relation remains readable after crash restart');

$node->stop;
done_testing();
