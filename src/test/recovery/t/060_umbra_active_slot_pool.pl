# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify default slot-0 selectors, pool replacement, and selector corruption.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub append_zero_block
{
	my ($path, $block_size) = @_;
	my $page = "\0" x $block_size;

	open(my $fh, '>>', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	print $fh $page or BAIL_OUT("could not append to \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

sub write_byte
{
	my ($path, $offset, $value) = @_;

	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek \"$path\": $!");
	print $fh pack('C', $value)
	  or BAIL_OUT("could not write \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

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
	defined(sysseek($fh, $offset, SEEK_SET))
	  or BAIL_OUT("could not seek \"$path\": $!");
	my $nread = sysread($fh, $byte, 1);
	defined($nread) && $nread == 1
	  or BAIL_OUT("could not read selector from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");

	return (unpack('C', $byte) >> (($entry_index % 4) * 2)) & 0x03;
}

my $node = PostgreSQL::Test::Cluster->new('active_slot_pool');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf',
	"shared_buffers = '128kB'\nautovacuum = off\ncheckpoint_timeout = '1h'\nmax_wal_size = '4GB'\nwal_log_hints = off");
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_selector_anchor (id integer) WITH (autovacuum_enabled = false);
INSERT INTO umbra_selector_anchor VALUES (1);
DO $$
BEGIN
  FOR i IN 1..24 LOOP
    EXECUTE format(
      'CREATE TABLE umbra_selector_pool_%s (id integer) WITH (autovacuum_enabled = false)',
      i);
    EXECUTE format('INSERT INTO umbra_selector_pool_%s VALUES (1)', i);
  END LOOP;
END
$$;
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $anchor_filenode = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('umbra_selector_anchor'::regclass)]);
my @relations = ('umbra_selector_anchor',
	map { "umbra_selector_pool_$_" } 1 .. 24);
my @map_paths;

for my $relation (@relations)
{
	my $main_path = $node->safe_psql(
		'postgres', "SELECT pg_relation_filepath('$relation'::regclass)");
	push @map_paths, $node->data_dir . "/${main_path}_map";
}

is(-s $map_paths[0], 4 * $block_size,
	'extension materializes the first zero selector group with the root');

# A restart forces a physical read through MapGetActiveSlot().  A missing
# selector page remains a valid all-slot-0 representation and reads must not
# recreate it.
$node->stop;
truncate($map_paths[0], $block_size)
  or BAIL_OUT("could not remove selector group from \"$map_paths[0]\": $!");
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_selector_anchor'),
	'1', 'an absent selector page reads as slot 0');
is(-s $map_paths[0], $block_size,
	'ordinary slot-0 reads do not materialize a selector page');

# A page that crosses the next redo boundary must choose its source and target
# before WAL insertion.  That synchronous choice materializes an absent
# selector page instead of falling back to an FPI.
$node->safe_psql('postgres', 'CHECKPOINT');
my $shift_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', 'UPDATE umbra_selector_anchor SET id = 2 WHERE id = 1');
my $shift_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is(-s $map_paths[0], 4 * $block_size,
	'WAL slot selection materializes the missing selector group');
my ($shift_wal, $shift_stderr) = run_command(
	[
		'pg_waldump', '--bkp-details',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $shift_wal_start,
		'--end' => $shift_wal_end,
	]);
$shift_stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
is($shift_stderr, '', 'pg_waldump reads the cache-miss update WAL');
my @shift_records = grep {
	/rel \d+\/\d+\/$anchor_filenode fork main blk 0\b/
} split(/\n/, $shift_wal);
my @selector_shifts = grep { /slot shift:/ } @shift_records;
is(scalar(@selector_shifts), 1,
	'cache-miss update records one selector transition');
unlike($selector_shifts[0] // '', qr/\bFPW\b/,
	'cache-miss selector transition is image-free');
like($selector_shifts[0] // '',
	qr/slot shift: source_slot 0 target_slot 1\b/,
	'cache-miss update records the slot 0 to slot 1 transition');
$node->safe_psql('postgres', 'CHECKPOINT');
is(read_active_slot($map_paths[0], $block_size, 0), 1,
	'checkpoint persists the selector chosen after a cache miss');

$node->stop;
for my $map_path (@map_paths)
{
	truncate($map_path, $block_size)
	  or BAIL_OUT("could not remove selector group from \"$map_path\": $!");
	append_zero_block($map_path, $block_size) for 1 .. 3;
}
$node->start;

# The pool minimum is 16 slots; 25 relation tags force replacement.
for my $relation (@relations)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM $relation"), '1',
		"zero selector page keeps $relation on slot 0");
}
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop;
$node->start;
for my $relation (@relations)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM $relation"), '1',
		"selector cache replacement survives restart for $relation");
}

$node->stop;
write_byte($map_paths[0], 3 * $block_size, 3);
$node->start;
my ($result, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT count(*) FROM umbra_selector_anchor');
is($result, 3, 'invalid selector rejects data access');
like($stderr, qr/invalid Umbra active slot 3/,
	'invalid selector is reported as metadata corruption');

$node->stop;

done_testing();
