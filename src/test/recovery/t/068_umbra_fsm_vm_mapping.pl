# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify three-bucket FSM/VM length, missing-fork redo, and truncate redo.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub relation_fork_blocks
{
	my ($node, $relation, $fork) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('$relation'::regclass, '$fork') / "
		  . "current_setting('block_size')::integer");
}

sub physical_blocks
{
	my ($path, $block_size) = @_;

	return 0 unless -e $path;
	return (-s $path) / $block_size;
}

sub check_fork_layout
{
	my ($node, $relation, $fork, $path, $block_size, $label) = @_;
	my $logical_eof = relation_fork_blocks($node, $relation, $fork);
	my $physical_eof = physical_blocks($path, $block_size);

	is($physical_eof, 3 * $logical_eof,
		"$label: $fork physical length is exactly three times logical EOF");
	return $logical_eof;
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
my $fsm_path = $node->data_dir . "/${main_path}_fsm";
my $vm_path = $node->data_dir . "/${main_path}_vm";

my $initial_fsm = check_fork_layout($node, 'umbra_aux_map', 'fsm',
	$fsm_path, $block_size, 'initial FSM');
my $initial_vm = check_fork_layout($node, 'umbra_aux_map', 'vm',
	$vm_path, $block_size, 'initial VM');
cmp_ok($initial_fsm, '>', 0, 'FSM fork was created');
cmp_ok($initial_vm, '>', 0, 'VM fork was created');

# Page WAL after this checkpoint must rebuild missing auxiliary files using
# the globally fixed three-bucket layout, without any relation root.
$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_aux_map
SELECT g, repeat('y', 1200) FROM generate_series(5001, 5500) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
});
$node->stop('immediate');
unlink $fsm_path or BAIL_OUT("could not remove \"$fsm_path\": $!");
unlink $vm_path or BAIL_OUT("could not remove \"$vm_path\": $!");
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '5500',
	'auxiliary redo recreates missing physical forks');
my $recovered_fsm = check_fork_layout($node, 'umbra_aux_map', 'fsm',
	$fsm_path, $block_size, 'missing-fork redo FSM');
my $recovered_vm = check_fork_layout($node, 'umbra_aux_map', 'vm',
	$vm_path, $block_size, 'missing-fork redo VM');
cmp_ok($recovered_fsm, '>', 0, 'missing-fork redo restores FSM extent');
cmp_ok($recovered_vm, '>', 0, 'missing-fork redo restores VM extent');

$node->safe_psql('postgres', 'CREATE EXTENSION pg_visibility');
$node->safe_psql('postgres',
	q{SELECT pg_truncate_visibility_map('umbra_aux_map'::regclass);});
is(check_fork_layout($node, 'umbra_aux_map', 'vm', $vm_path, $block_size,
		'pg_visibility truncate'), 0,
	'pg_visibility truncates VM through its physical EOF');

$node->safe_psql('postgres',
	q{VACUUM (FREEZE, ANALYZE) umbra_aux_map; CHECKPOINT;});
my $pretruncate_fsm = check_fork_layout($node, 'umbra_aux_map', 'fsm',
	$fsm_path, $block_size, 'pre-truncate FSM');
my $pretruncate_vm = check_fork_layout($node, 'umbra_aux_map', 'vm',
	$vm_path, $block_size, 'pre-truncate VM');
cmp_ok($pretruncate_vm, '>', 0, 'VM rebuild creates a logical page');
my $fsm_before_truncate = slurp_file($fsm_path);
my $vm_before_truncate = slurp_file($vm_path);

$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_aux_map;
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_aux_map;
});
my $truncated_fsm = check_fork_layout($node, 'umbra_aux_map', 'fsm',
	$fsm_path, $block_size, 'post-truncate FSM');
my $truncated_vm = check_fork_layout($node, 'umbra_aux_map', 'vm',
	$vm_path, $block_size, 'post-truncate VM');
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
is(check_fork_layout($node, 'umbra_aux_map', 'fsm', $fsm_path, $block_size,
		'truncate redo FSM'), $truncated_fsm,
	'truncate redo removes the stale FSM physical tail');
is(check_fork_layout($node, 'umbra_aux_map', 'vm', $vm_path, $block_size,
		'truncate redo VM'), $truncated_vm,
	'truncate redo removes the stale VM physical tail');

$node->stop;
done_testing();
