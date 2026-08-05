# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify FSM and VM active-slot rotation, checkpoint/crash persistence, and
# nonzero-slot truncate/regrow behavior.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

sub read_active_slot
{
	my ($path, $block_size, $fork, $logical_block) = @_;
	my $entries_per_page = $block_size * 4;
	my $page_index = int($logical_block / $entries_per_page);
	my $entry_index = $logical_block % $entries_per_page;
	my $map_block = $fork eq 'fsm' ? 1 + $page_index * 258
	  : $fork eq 'vm'              ? 2 + $page_index * 258
	  : BAIL_OUT("unsupported auxiliary fork \"$fork\"");
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

sub update_round
{
	my ($node, $marker, $label, $relation) = @_;
	$relation //= 'umbra_aux_slot_shift';
	my $start_lsn = $node->safe_psql(
		'postgres', 'SELECT pg_current_wal_insert_lsn();');

	$node->safe_psql(
		'postgres',
		"UPDATE $relation SET payload = repeat('$marker', 500);");
	$node->safe_psql(
		'postgres',
		'VACUUM (FREEZE, ANALYZE, DISABLE_PAGE_SKIPPING) '
		  . "$relation;");
	$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
	my $end_lsn = $node->safe_psql(
		'postgres', 'SELECT pg_current_wal_flush_lsn();');
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	is(normalize_waldump_stderr($stderr), '',
		"$label: pg_waldump reads the update and vacuum WAL");
	return [split(/(?=^rmgr: )/m, $dump)];
}

sub transition_blocks
{
	my ($records, $filenode, $fork, $source_slot, $target_slot) = @_;
	my %blocks;

	for my $record (@$records)
	{
		next if $fork eq 'fsm' && $record !~ /desc: FPI_FOR_HINT/;
		while ($record =~
			/^(.*rel \d+\/\d+\/\Q$filenode\E fork \Q$fork\E blk (\d+)\b.*)$/mg)
		{
			my ($line, $block) = ($1, $2);

			if ($fork eq 'fsm')
			{
				next unless $line =~ /\bFPW\b/;
			}
			else
			{
				next if $line =~ /\bFPW\b/;
			}
			next unless $line =~
			  /slot shift: source_slot $source_slot target_slot $target_slot\b/;
			$blocks{$block} = 1;
		}
	}
	return \%blocks;
}

sub fsm_snapshot
{
	my $node = shift;

	return $node->safe_psql(
		'postgres', q[
SELECT COALESCE(string_agg(blkno::text || ':' || avail::text,
                           ',' ORDER BY blkno), '')
FROM pg_freespace('umbra_aux_slot_shift'::regclass);]);
}

sub vm_snapshot
{
	my $node = shift;

	return $node->safe_psql(
		'postgres', q[
SELECT COALESCE(string_agg(blkno::text || ':' || all_visible::text || ':' ||
                           all_frozen::text, ',' ORDER BY blkno), '')
FROM pg_visibility_map('umbra_aux_slot_shift'::regclass);]);
}

my $node = PostgreSQL::Test::Cluster->new('umbra_aux_slot_shift');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
mapwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = on
log_min_messages = debug1
max_wal_size = '4GB'
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION pg_freespacemap;
CREATE EXTENSION pg_visibility;
CREATE TABLE umbra_aux_slot_shift(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_aux_slot_shift
SELECT g, repeat('x', 500) FROM generate_series(1, 500) AS g;
]);
$node->safe_psql(
	'postgres', 'VACUUM (FREEZE, ANALYZE) umbra_aux_slot_shift;');
$node->safe_psql('postgres', 'CHECKPOINT;');
$node->append_conf('postgresql.conf', "full_page_writes = on\n");
$node->stop;
$node->start;
is($node->safe_psql('postgres', 'SHOW full_page_writes'), 'on',
	'auxiliary rotation runs with full-page writes enabled');
$node->safe_psql('postgres', 'CHECKPOINT;');

my $filenode = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('umbra_aux_slot_shift'::regclass);]);
my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_aux_slot_shift'::regclass);]);
my $map_path = $node->data_dir . "/${relpath}_map";
cmp_ok($node->safe_psql(
		'postgres',
		q[SELECT pg_relation_size('umbra_aux_slot_shift', 'fsm');]),
	'>', 0, 'FSM fork exists before rotation');
cmp_ok($node->safe_psql(
		'postgres',
		q[SELECT pg_relation_size('umbra_aux_slot_shift', 'vm');]),
	'>', 0, 'VM fork exists before rotation');
my @transitions = ([0, 1], [1, 2], [2, 0]);
my %tracked_block;
for my $round (0 .. 2)
{
	my $marker = chr(ord('a') + $round);
	my ($source_slot, $target_slot) = @{ $transitions[$round] };
	my $records =
	  update_round($node, $marker, 'auxiliary round ' . ($round + 1));

	for my $fork ('fsm', 'vm')
	{
		my $wal_shape = $fork eq 'fsm' ? 'FPI-backed' : 'image-free';
		my $found = transition_blocks(
			$records, $filenode, $fork, $source_slot, $target_slot);

		ok(keys(%$found) > 0,
			"$fork round " . ($round + 1)
			  . " records $wal_shape slot $source_slot to $target_slot");
		if ($round == 0)
		{
			($tracked_block{$fork}) = sort { $a <=> $b } keys(%$found);
		}
		else
		{
			ok(exists $found->{ $tracked_block{$fork} },
				"$fork keeps rotating logical block $tracked_block{$fork}");
		}
	}

	if ($round == 0)
	{
		my $fsm_before_nonzero_recovery = fsm_snapshot($node);
		my $vm_before_nonzero_recovery = vm_snapshot($node);

		$node->stop('immediate');
		$node->start;
		for my $fork ('fsm', 'vm')
		{
			is(read_active_slot($map_path, $block_size, $fork,
					$tracked_block{$fork}),
				1, "$fork crash redo persists nonzero selector slot 1");
		}
		is(fsm_snapshot($node), $fsm_before_nonzero_recovery,
			'crash redo restores FSM contents in nonzero slot 1');
		is(vm_snapshot($node), $vm_before_nonzero_recovery,
			'crash redo restores VM contents in nonzero slot 1');
		is($node->safe_psql(
				'postgres',
				q[SELECT count(*) FROM umbra_aux_slot_shift
                  WHERE payload <> repeat('a', 500);]),
			'0', 'crash redo restores first-round table contents');
	}

	if ($round < 2)
	{
		$node->safe_psql('postgres', 'CHECKPOINT');
		for my $fork ('fsm', 'vm')
		{
			is(read_active_slot($map_path, $block_size, $fork,
					$tracked_block{$fork}),
				$target_slot,
				"$fork checkpoint persists selector slot $target_slot");
		}

		# Force the next round to reload selector pages instead of reusing the
		# shared MAP cache that observed the preceding publication.
		$node->stop;
		$node->start;
	}
}

my $fsm_before_crash = fsm_snapshot($node);
my $vm_before_crash = vm_snapshot($node);
ok(length($fsm_before_crash) > 0,
	'FSM snapshot is nonempty before crash recovery');
ok(length($vm_before_crash) > 0,
	'VM snapshot is nonempty before crash recovery');

# The third target has not been checkpointed.  Redo must reconstruct slot 0
# through each fork's recorded transition before later auxiliary access.
$node->stop('immediate');
$node->start;
for my $fork ('fsm', 'vm')
{
	is(read_active_slot($map_path, $block_size, $fork,
			$tracked_block{$fork}),
		0, "$fork crash redo persists selector slot 0");
}
is(fsm_snapshot($node), $fsm_before_crash,
	'crash redo restores FSM contents before later maintenance');
is(vm_snapshot($node), $vm_before_crash,
	'crash redo restores VM contents before later maintenance');
is($node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM umbra_aux_slot_shift
          WHERE payload <> repeat('c', 500);]),
	'0', 'crash redo restores the third-round table contents');

$node->safe_psql('postgres', 'CHECKPOINT');
my $after_recovery = update_round($node, 'd', 'post-recovery auxiliary round');
my $fsm_after = transition_blocks($after_recovery, $filenode, 'fsm', 0, 1);
my $vm_after = transition_blocks($after_recovery, $filenode, 'vm', 0, 1);
ok(exists $fsm_after->{ $tracked_block{fsm} },
	'FSM crash redo leaves the rotated selector at slot 0');
ok(exists $vm_after->{ $tracked_block{vm} },
	'VM crash redo leaves the rotated selector at slot 0');

# Applying-FPI redo must not treat a missing MAP root as lifecycle authority.
# An FSM FPI_FOR_HINT records the dependency, and the later DROP clears it.
$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_aux_slot_shift_drop(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_aux_slot_shift_drop
SELECT g, repeat('x', 500) FROM generate_series(1, 500) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_slot_shift_drop;
CHECKPOINT;
]);
my $drop_filenode = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('umbra_aux_slot_shift_drop'::regclass);]);
my $drop_relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_aux_slot_shift_drop'::regclass);]);
my $drop_map_path = $node->data_dir . "/${drop_relpath}_map";
my $drop_records = update_round(
	$node, 'm', 'auxiliary missing-root round',
	'umbra_aux_slot_shift_drop');
my $drop_fsm_block;
for my $transition (@transitions)
{
	my ($source_slot, $target_slot) = @$transition;
	my $found = transition_blocks(
		$drop_records, $drop_filenode, 'fsm', $source_slot, $target_slot);

	if (keys(%$found) > 0)
	{
		($drop_fsm_block) = sort { $a <=> $b } keys(%$found);
		last;
	}
}
ok(defined($drop_fsm_block),
	'missing-root relation emits an FSM applying-FPI shift')
  or BAIL_OUT('could not identify the applying-FPI FSM block');
$node->safe_psql('postgres', 'DROP TABLE umbra_aux_slot_shift_drop;');
ok(!-e $drop_map_path,
	'later DROP removes the applying-FPI shift mapping before crash');
my $drop_recovery_log_offset = -s $node->logfile;
$node->stop('immediate');
$node->start;
ok($node->log_contains(
		qr/page $drop_fsm_block of relation .*\/${drop_filenode}_fsm does not exist/,
		$drop_recovery_log_offset),
	'missing MAP root records the FSM FPI shift as a dependency');
is($node->safe_psql(
		'postgres', q[SELECT to_regclass('umbra_aux_slot_shift_drop') IS NULL;]),
	't', 'later DROP clears the FSM missing-root dependency');

# Truncate must preserve a nonzero selector without exposing old auxiliary
# bytes when the same logical page is regrown.  Disable FPWs for the regrow so
# a fresh selector shift cannot hide an incorrect active-slot initialization.
$node->safe_psql('postgres', 'CHECKPOINT');
for my $fork ('fsm', 'vm')
{
	is(read_active_slot($map_path, $block_size, $fork,
			$tracked_block{$fork}),
		1, "$fork selector is nonzero before truncate");
}
$node->safe_psql(
	'postgres', q[
DELETE FROM umbra_aux_slot_shift;
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_aux_slot_shift;
CHECKPOINT;
]);
my %retained_slot;
for my $fork ('fsm', 'vm')
{
	$retained_slot{$fork} = read_active_slot(
		$map_path, $block_size, $fork, $tracked_block{$fork});
	cmp_ok($retained_slot{$fork}, '>', 0,
		"$fork truncate retains a nonzero selector");
	cmp_ok($retained_slot{$fork}, '<', 3,
		"$fork retained selector remains valid");
}

$node->append_conf('postgresql.conf', "full_page_writes = off\n");
$node->stop;
$node->start;
is($node->safe_psql('postgres', 'SHOW full_page_writes'), 'off',
	'regrow runs without redo-boundary selector shifts');
$node->safe_psql(
	'postgres', q[
INSERT INTO umbra_aux_slot_shift
SELECT g, repeat('r', 500) FROM generate_series(1, 500) AS g;
VACUUM (FREEZE, ANALYZE, DISABLE_PAGE_SKIPPING) umbra_aux_slot_shift;
CHECKPOINT;
]);
for my $fork ('fsm', 'vm')
{
	is(read_active_slot($map_path, $block_size, $fork,
			$tracked_block{$fork}),
		$retained_slot{$fork},
		"$fork regrow initializes the retained nonzero slot");
}
my $fsm_after_regrow = fsm_snapshot($node);
my $vm_after_regrow = vm_snapshot($node);
$node->stop('immediate');
$node->start;
is(fsm_snapshot($node), $fsm_after_regrow,
	'nonzero-slot FSM regrow survives crash restart');
is(vm_snapshot($node), $vm_after_regrow,
	'nonzero-slot VM regrow survives crash restart');
is($node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM umbra_aux_slot_shift
          WHERE payload = repeat('r', 500);]),
	'500', 'nonzero-slot regrown relation survives crash restart');

$node->stop;
done_testing();
