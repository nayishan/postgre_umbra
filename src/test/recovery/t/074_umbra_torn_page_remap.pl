# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that Umbra crash recovery can recover a remapped heap page even when
# the newly allocated physical block contains a torn write image.  In md mode,
# run the same workload with full_page_writes=off as a negative control: the
# manually torn heap page must not be recoverable as correct data.
#
# The test:
# - checkpoints a relation, then updates existing heap pages
# - in Umbra mode, extracts one new physical block number from the remap WAL
# - in md mode, extracts one updated heap block as the negative control target
# - kills the server, overwrites half of that physical block, and restarts
# - verifies Umbra restores the logical relation contents while md/FPW-off
#   cannot recover the torn page as correct data
use strict;
use warnings FATAL => 'all';

use Fcntl qw(O_CREAT O_RDWR SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $use_umbra = check_pg_config('^#define USE_UMBRA 1$');

sub overwrite_half_physical_block
{
	my ($node, $relpath, $block_size, $pblkno) = @_;

	my $seg_blocks = int((1024 * 1024 * 1024) / $block_size);
	my $segno = int($pblkno / $seg_blocks);
	my $segblk = $pblkno % $seg_blocks;
	my $path = $node->data_dir . '/' . $relpath . ($segno == 0 ? '' : ".$segno");
	my $offset = $segblk * $block_size;
	my $zeros = "\0" x int($block_size / 2);

	sysopen(my $fh, $path, O_RDWR | O_CREAT, 0600)
	  or die "could not open $path: $!";
	binmode($fh);
	defined(sysseek($fh, $offset, SEEK_SET))
	  or die "could not seek to $offset in $path: $!";
	my $written = syswrite($fh, $zeros);
	die "could not overwrite torn half-page in $path: $!"
	  unless defined($written) && $written == length($zeros);
	close($fh) or die "could not close $path: $!";

	return ($path, $offset);
}

sub setup_node
{
	my ($name, $fpw) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);

	$node->init();
	$node->append_conf(
		'postgresql.conf', qq[
autovacuum = off
full_page_writes = $fpw
shared_buffers = '256MB'
max_wal_size = '4GB'
min_wal_size = '1GB'
checkpoint_timeout = '1h'
]);
	$node->start();
	return $node;
}

sub prepare_and_update_table
{
	my ($node) = @_;

	$node->safe_psql('postgres', q[
CREATE TABLE umb_torn_page_t(id bigint, payload text)
  WITH (fillfactor = 70);
INSERT INTO umb_torn_page_t
SELECT g, repeat('x', 80)
FROM generate_series(1, 200000) AS g;
CHECKPOINT;
]);

	my $relinfo = $node->safe_psql('postgres', q[
SELECT (CASE WHEN c.reltablespace = 0
             THEN d.dattablespace
             ELSE c.reltablespace
        END)::text || '/' ||
       d.oid::text || '/' ||
       pg_relation_filenode(c.oid)::text || '|' ||
       pg_relation_filepath(c.oid) || '|' ||
       current_setting('block_size')
FROM pg_class c
JOIN pg_database d ON d.datname = current_database()
WHERE c.oid = 'umb_torn_page_t'::regclass;
]);
	my ($locator, $relpath, $block_size) = split /\|/, $relinfo;

	my $start_lsn =
	  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

	$node->safe_psql('postgres', q[
UPDATE umb_torn_page_t
SET payload = md5(id::text) || repeat('u', 48)
WHERE id <= 100000;
]);

	my $before = $node->safe_psql('postgres', relation_signature_sql());

	my $end_lsn =
	  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

	my ($dump_stdout, $dump_stderr) = run_command(
		[
			'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
			'--start', $start_lsn,
			'--end',   $end_lsn
		]);
	$dump_stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes\n?//m;
	is($dump_stderr, '',
		'pg_waldump block dump completed without unexpected stderr');

	return ($locator, $relpath, $block_size, $before, $dump_stdout);
}

sub relation_signature_sql
{
	return q[
SELECT count(*) || ',' ||
       md5(string_agg(md5(id::text || ':' || payload), '' ORDER BY id))
FROM umb_torn_page_t;
];
}

sub find_umbra_remap
{
	my ($locator, $dump_stdout) = @_;

	foreach my $blkref (split /(?=blkref #\d+:)/, $dump_stdout)
	{
		next
		  unless $blkref =~ /blkref #\d+: rel \Q$locator\E fork main blk (\d+)/;
		my $lblk = $1;
		next
		  unless $blkref =~
		  /; remap: old_pblk (\d+) new_pblk (\d+) logical_nblocks \d+ next_free_pblk \d+/;

		my ($old, $new) = ($1, $2);
		next if $old == 4294967295;
		next if $old == $new;

		return ($lblk, $old, $new);
	}

	return;
}

sub find_md_heap_block
{
	my ($locator, $dump_stdout) = @_;

	foreach my $blkref (split /(?=blkref #\d+:)/, $dump_stdout)
	{
		next
		  unless $blkref =~ /blkref #\d+: rel \Q$locator\E fork main blk (\d+)/;
		my $lblk = $1;
		next if $lblk == 0;
		return $lblk;
	}

	return;
}

sub verify_table_contents
{
	my ($node, $before) = @_;

	my $after = $node->safe_psql('postgres', relation_signature_sql());

	is($after, $before,
		'recovery restores relation contents after torn new physical block');

	is($node->safe_psql(
			'postgres',
			q[SELECT count(*) FROM umb_torn_page_t
			   WHERE id <= 100000
			     AND left(payload, 8) = left(md5(id::text), 8);]),
		'100000',
		'updated rows are visible after recovery');

	is($node->safe_psql(
			'postgres',
			q[SELECT count(*) FROM umb_torn_page_t
			   WHERE id > 100000
			     AND payload = repeat('x', 80);]),
		'100000',
		'unmodified rows remain visible after recovery');
}

if ($use_umbra)
{
	my $node = setup_node('umbra_torn_page_remap', 'on');
	my ($locator, $relpath, $block_size, $before, $dump_stdout) =
	  prepare_and_update_table($node);

	my ($target_lblk, $old_pblk, $new_pblk) =
	  find_umbra_remap($locator, $dump_stdout);

	ok(defined($new_pblk),
		'update WAL contains a heap remap header with a new physical block');
	BAIL_OUT('could not locate a concrete Umbra remap block for test relation')
		unless defined($new_pblk);
	cmp_ok($new_pblk, '!=', $old_pblk,
		'selected WAL remap moves the heap page to a different physical block');

	$node->stop('immediate');

	my ($corrupt_path, $corrupt_offset) =
	  overwrite_half_physical_block($node, $relpath, $block_size, $new_pblk);
	ok(-e $corrupt_path,
		'new physical block segment exists after torn-write injection');
	ok($corrupt_offset >= 0,
		'torn-write injection targeted a concrete physical offset');

	$node->start();
	verify_table_contents($node, $before);
}
else
{
	my $node = setup_node('md_torn_page_fpw_off', 'off');
	my ($locator, $relpath, $block_size, $before, $dump_stdout) =
	  prepare_and_update_table($node);

	my $target_lblk = find_md_heap_block($locator, $dump_stdout);
	ok(defined($target_lblk),
		'update WAL contains a heap block reference for md negative control');
	BAIL_OUT('could not locate a concrete md heap block for test relation')
		unless defined($target_lblk);

	$node->stop('immediate');

	my ($corrupt_path, $corrupt_offset) =
	  overwrite_half_physical_block($node, $relpath, $block_size, $target_lblk);
	ok(-e $corrupt_path,
		'md heap segment exists after torn-write injection');
	ok($corrupt_offset >= 0,
		'md torn-write injection targeted a concrete physical offset');

	my $started = $node->start(fail_ok => 1);
	if (!$started)
	{
		pass('md with full_page_writes=off cannot restart from the torn page');
	}
	else
	{
		my ($ret, $stdout, $stderr) =
		  $node->psql('postgres', relation_signature_sql());
		ok($ret != 0 || $stdout ne $before,
			'md with full_page_writes=off does not recover correct data from the torn page');
	}
}

done_testing();
