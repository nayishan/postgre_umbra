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

my $node = PostgreSQL::Test::Cluster->new('active_slot_pool');
$node->init;
$node->append_conf(
	'postgresql.conf',
	"shared_buffers = '128kB'\nautovacuum = off\ncheckpoint_timeout = '1h'\nmax_wal_size = '4GB'");
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

# A missing selector represents slot zero until the first shift materializes
# it.  Precreation keeps the normal extension path outside the WAL critical
# section, but this recovery-compatible case remains valid.
$node->safe_psql('postgres', 'CHECKPOINT');
my $shift_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', 'UPDATE umbra_selector_anchor SET id = 2 WHERE id = 1');
my $shift_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is(-s $map_paths[0], 4 * $block_size,
	'slot shift materializes the missing selector group');
my ($shift_wal, $shift_stderr) = run_command(
	[
		'pg_waldump', '--bkp-details',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $shift_wal_start,
		'--end' => $shift_wal_end,
	]);
$shift_stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
is($shift_stderr, '', 'pg_waldump reads the missing-selector update WAL');
my @shift_records = grep {
	/rel \d+\/\d+\/$anchor_filenode fork main blk 0\b/
} split(/\n/, $shift_wal);
my @selector_shifts = grep { /slot shift:/ } @shift_records;
is(scalar(@selector_shifts), 1,
	'missing-selector update records one selector transition');
like($selector_shifts[0] // '',
	qr/slot shift: source_slot 0 target_slot 1\b/,
	'missing-selector update advances the default selector');
$node->safe_psql('postgres', 'CHECKPOINT');
is(-s $map_paths[0], 4 * $block_size,
	'checkpoint retains the materialized selector group');
is($node->safe_psql('postgres', 'SELECT id FROM umbra_selector_anchor'),
	'2', 'slot shift preserves the updated page');

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
