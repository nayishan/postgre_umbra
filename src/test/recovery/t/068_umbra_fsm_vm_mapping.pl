# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify three-bucket FSM/VM length, missing-fork redo, and truncate redo.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub physical_blocks
{
	my ($path, $block_size) = @_;

	return 0 unless -e $path;
	return (-s $path) / $block_size;
}

sub check_fork_layout
{
	my ($fork, $path, $block_size, $label) = @_;
	my $physical_eof = physical_blocks($path, $block_size);

	is($physical_eof % 3, 0,
		"$label: $fork has a complete three-bucket physical tail");
	return $physical_eof / 3;
}

sub assert_aux_shift_wal
{
	my ($node, $filenode, $fork, $start_lsn, $end_lsn) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', "$fork WAL decodes");
	my @shifts = grep {
		/rel \d+\/\d+\/$filenode fork $fork blk \d+.*slot shift:/
	} split(/\n/, $dump);
	cmp_ok(scalar(@shifts), '>', 0, "$fork WAL has a target-only slot shift");
	unlike($shifts[0] // '', qr/\bFPW\b/,
		"$fork target-only shift omits the full-page image");
}

sub assert_vm_zero_source_wal
{
	my ($node, $filenode, $start_lsn, $end_lsn) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', 'ordinary VM WAL decodes');
	my @records = split(/(?=^rmgr: )/m, $dump);
	my @vm_records = grep {
		/rmgr: Heap2/ &&
		/rel \d+\/\d+\/$filenode fork vm blk \d+/ &&
		/slot shift: source_slot 0 target_slot [1-2]\b/
	} @records;
	cmp_ok(scalar(@vm_records), '>', 0,
		'ordinary VM WAL starts from source slot zero');
	unlike($vm_records[0] // '', qr/\bFPW\b/,
		'ordinary VM WAL uses target-only redo');
}

sub overwrite_binary_file
{
	my ($path, $contents) = @_;

	open(my $fh, '>', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	print $fh $contents or BAIL_OUT("could not write \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

my $node = PostgreSQL::Test::Cluster->new('fsm_vm_three_buckets');
$node->init(no_data_checksums => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '4GB'
]);
$node->start;

$node->safe_psql(
	'postgres', q{
CHECKPOINT;
CREATE TABLE umbra_aux_map (id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_aux_map
SELECT g, repeat('x', 1200) FROM generate_series(1, 5000) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_aux_map'::regclass);});
my $filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_aux_map'::regclass);});
my $fsm_path = $node->data_dir . "/${main_path}_fsm";
my $vm_path = $node->data_dir . "/${main_path}_vm";

my $initial_fsm = check_fork_layout('fsm', $fsm_path, $block_size,
	'initial FSM');
my $initial_vm = check_fork_layout('vm', $vm_path, $block_size,
	'initial VM');
cmp_ok($initial_fsm, '>', 0, 'FSM fork was created');
cmp_ok($initial_vm, '>', 0, 'VM fork was created');

# The first VM page for a relation can be born in ordinary Heap2 redo.  Its
# source is the all-zero slot-zero page, so recovery must recreate that source
# when the physical VM fork is missing.
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_aux_zero_vm (id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_aux_zero_vm
SELECT g, repeat('z', 1200) FROM generate_series(1, 20) AS g;
CHECKPOINT;
});
my $zero_vm_relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_aux_zero_vm'::regclass);});
my $zero_vm_filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('umbra_aux_zero_vm'::regclass);});
my $zero_vm_path = $node->data_dir . "/${zero_vm_relpath}_vm";
ok(!-e $zero_vm_path, 'new relation starts without a VM fork');
my $zero_vm_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q{VACUUM (FREEZE, ANALYZE) umbra_aux_zero_vm;});
$node->safe_psql('postgres', 'SELECT pg_switch_wal();');
my $zero_vm_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn();');
assert_vm_zero_source_wal($node, $zero_vm_filenode,
	$zero_vm_wal_start, $zero_vm_wal_end);
ok(-e $zero_vm_path, 'ordinary VM redo created the VM fork');

$node->stop('immediate');
unlink $zero_vm_path or BAIL_OUT("could not remove \"$zero_vm_path\": $!");
ok(!-e $zero_vm_path, 'VM fork is missing before ordinary redo');
$node->start;
is($node->safe_psql('postgres',
	'SELECT count(*) FROM umbra_aux_zero_vm'), '20',
	'ordinary VM redo preserves the relation');
my $recovered_zero_vm = check_fork_layout('vm', $zero_vm_path,
	$block_size, 'ordinary VM zero-source redo');
cmp_ok($recovered_zero_vm, '>', 0,
	'ordinary VM redo rebuilds the missing physical fork');

# Page WAL after this checkpoint shifts existing auxiliary pages without an
# image.  Target-only recovery relies on those source slots remaining present.
my $aux_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_aux_map
SELECT g, repeat('y', 1200) FROM generate_series(5001, 5500) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
});
my $aux_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
# FSM updates use XLOG_FPI_FOR_HINT and therefore retain their forced image.
assert_aux_shift_wal($node, $filenode, 'vm', $aux_wal_start, $aux_wal_end);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '5500',
	'auxiliary target-only redo restores the heap');
my $recovered_fsm = check_fork_layout('fsm', $fsm_path, $block_size,
	'target-only redo FSM');
my $recovered_vm = check_fork_layout('vm', $vm_path, $block_size,
	'target-only redo VM');
cmp_ok($recovered_fsm, '>', 0, 'target-only redo preserves FSM extent');
cmp_ok($recovered_vm, '>', 0, 'target-only redo preserves VM extent');

$node->safe_psql('postgres',
	q{VACUUM (FREEZE, ANALYZE) umbra_aux_map; CHECKPOINT;});
my $pretruncate_fsm = check_fork_layout('fsm', $fsm_path, $block_size,
	'pre-truncate FSM');
my $pretruncate_vm = check_fork_layout('vm', $vm_path, $block_size,
	'pre-truncate VM');
cmp_ok($pretruncate_vm, '>', 0, 'VM rebuild creates a logical page');
my $fsm_before_truncate = slurp_file($fsm_path);
my $vm_before_truncate = slurp_file($vm_path);

$node->safe_psql('postgres', 'TRUNCATE umbra_aux_map;');
my $truncated_fsm = check_fork_layout('fsm', $fsm_path, $block_size,
	'post-truncate FSM');
my $truncated_vm = check_fork_layout('vm', $vm_path, $block_size,
	'post-truncate VM');
cmp_ok($truncated_fsm, '<=', $pretruncate_fsm,
	'truncate does not grow FSM logical EOF');
cmp_ok($truncated_vm, '<', $pretruncate_vm,
	'truncate lowers VM logical EOF');

# Restore old tails after an immediate stop.  Redo must derive and enforce
# the exact target physical lengths from the truncate record itself.
$node->stop('immediate');
overwrite_binary_file($fsm_path, $fsm_before_truncate);
overwrite_binary_file($vm_path, $vm_before_truncate);
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '0',
	'auxiliary truncate redo preserves the empty table');
is(check_fork_layout('fsm', $fsm_path, $block_size, 'truncate redo FSM'),
	$truncated_fsm,
	'truncate redo removes the stale FSM physical tail');
is(check_fork_layout('vm', $vm_path, $block_size, 'truncate redo VM'),
	$truncated_vm,
	'truncate redo removes the stale VM physical tail');

$node->stop;
done_testing();
