# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that unlogged reset removes stale selector state before INIT is copied.

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
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek in \"$path\": $!");
	sysread($fh, $byte, 1) == 1
	  or BAIL_OUT("could not read selector from \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return unpack('C', $byte) & 0x03;
}

my $node = PostgreSQL::Test::Cluster->new('unlogged_selector_reset');
$node->init(no_data_checksums => 1);
$node->start;
$node->safe_psql(
	'postgres', q{
CREATE UNLOGGED TABLE umbra_unlogged_selector (id integer PRIMARY KEY);
INSERT INTO umbra_unlogged_selector VALUES (1);
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umbra_unlogged_selector'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";

is(-s $map_path, 3 * $block_size,
	'unlogged MAIN initially has a selector group');

# Leave an impossible old selector behind.  After restart the unlogged MAIN
# comes from INIT, so this selector must not remain to redirect new pages.
$node->stop('immediate');
open(my $map_fh, '+<', $map_path)
  or BAIL_OUT("could not open \"$map_path\": $!");
binmode($map_fh);
defined(sysseek($map_fh, 2 * $block_size, SEEK_SET))
  or BAIL_OUT("could not seek in \"$map_path\": $!");
print $map_fh pack('C', 1)
  or BAIL_OUT("could not write stale selector to \"$map_path\": $!");
close($map_fh) or BAIL_OUT("could not close \"$map_path\": $!");

$node->start;
is($node->safe_psql('postgres',
		'SELECT count(*) FROM umbra_unlogged_selector'), '0',
	'unlogged contents are restored from INIT after a crash');
ok(!-e $map_path,
	'unlogged reset removes the selector-MAP left by the former MAIN');

$node->safe_psql('postgres',
	'INSERT INTO umbra_unlogged_selector VALUES (2)');
is(-s $map_path, 3 * $block_size,
	'new unlogged MAIN materializes a fresh selector group');
is(read_active_slot($map_path, $block_size, 0), 0,
	'fresh unlogged logical block starts in slot 0');

$node->stop;
done_testing();
