# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify mapped FSM/VM root frontiers, redo birth, zero-on-error reads, and
# truncate publication for Umbra auxiliary forks.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant CHUNK_PAGES => 32;
use constant CHUNK_SLOTS => 3;
use constant ROOT_FLAG_MAIN_SLOT0 => 0x00000002;
use constant ROOT_FLAG_FSM_SLOT0 => 0x00000004;
use constant ROOT_FLAG_VM_SLOT0 => 0x00000008;
use constant ROOT_FLAGS_OFFSET => 16;
use constant ROOT_LOGICAL_FSM_OFFSET => 24;
use constant ROOT_LOGICAL_VM_OFFSET => 28;
use constant ROOT_PHYSICAL_FSM_OFFSET => 44;
use constant ROOT_PHYSICAL_VM_OFFSET => 48;

sub expected_capacity
{
	my $logical_eof = shift;

	return 0 if $logical_eof == 0;
	return int(($logical_eof + CHUNK_PAGES - 1) / CHUNK_PAGES)
	  * CHUNK_PAGES * CHUNK_SLOTS;
}

sub root_u32
{
	my ($root, $offset) = @_;

	return unpack('L', substr($root, $offset, 4));
}

sub aux_root_state
{
	my ($map_path) = @_;
	my $root = slurp_file($map_path);

	return {
		flags => root_u32($root, ROOT_FLAGS_OFFSET),
		fsm_eof => root_u32($root, ROOT_LOGICAL_FSM_OFFSET),
		vm_eof => root_u32($root, ROOT_LOGICAL_VM_OFFSET),
		fsm_capacity => root_u32($root, ROOT_PHYSICAL_FSM_OFFSET),
		vm_capacity => root_u32($root, ROOT_PHYSICAL_VM_OFFSET),
	};
}

sub check_aux_layout
{
	my ($node, $main_path, $block_size, $label, $require_aux_pages) = @_;
	my $map_path = $node->data_dir . "/${main_path}_map";
	my $state = aux_root_state($map_path);
	my $fsm_path = $node->data_dir . "/${main_path}_fsm";
	my $vm_path = $node->data_dir . "/${main_path}_vm";

	ok(($state->{flags} & ROOT_FLAG_MAIN_SLOT0) != 0,
		"$label: root retains MAIN slot-0 mapping");
	ok(($state->{flags} & ROOT_FLAG_FSM_SLOT0) != 0,
		"$label: root activates FSM slot-0 mapping");
	ok(($state->{flags} & ROOT_FLAG_VM_SLOT0) != 0,
		"$label: root activates VM slot-0 mapping");
	if ($require_aux_pages)
	{
		cmp_ok($state->{fsm_eof}, '>', 0,
			"$label: FSM logical EOF is tracked");
		cmp_ok($state->{vm_eof}, '>', 0,
			"$label: VM logical EOF is tracked");
	}
	is($state->{fsm_capacity}, expected_capacity($state->{fsm_eof}),
		"$label: FSM capacity follows the mapped three-slot formula");
	is($state->{vm_capacity}, expected_capacity($state->{vm_eof}),
		"$label: VM capacity follows the mapped three-slot formula");
	is(-s $fsm_path, $state->{fsm_capacity} * $block_size,
		"$label: FSM physical file reaches its published capacity");
	is(-s $vm_path, $state->{vm_capacity} * $block_size,
		"$label: VM physical file reaches its published capacity");

	return $state;
}

sub overwrite_binary_file
{
	my ($path, $contents) = @_;

	open(my $fh, '>', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	print $fh $contents or BAIL_OUT("could not write \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

sub corrupt_first_page
{
	my ($path) = @_;

	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	seek($fh, 0, 0) or BAIL_OUT("could not seek \"$path\": $!");
	print $fh "\xFF" x 64
	  or BAIL_OUT("could not corrupt \"$path\": $!");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

my $node = PostgreSQL::Test::Cluster->new('fsm_vm_mapping');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_method=worker
io_worker_idle_timeout=0ms
io_worker_launch_interval=0ms
io_min_workers=1
autovacuum=off
checkpoint_timeout=1h
max_wal_size=4GB
));
$node->start;

is($node->safe_psql('postgres', 'SHOW io_method'), 'worker',
	'worker AIO is enabled');

# Persist a root that knows only MAIN, then leave auxiliary birth in WAL.
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_aux_map (id integer, payload text)
  WITH (autovacuum_enabled = false);
CHECKPOINT;
});

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_aux_map'::regclass);});
my $map_path = $node->data_dir . "/${main_path}_map";
my $fsm_path = $node->data_dir . "/${main_path}_fsm";
my $vm_path = $node->data_dir . "/${main_path}_vm";
my $root_without_aux = slurp_file($map_path);

is(root_u32($root_without_aux, ROOT_FLAGS_OFFSET), ROOT_FLAG_MAIN_SLOT0 | 1,
	'checkpointed root has not activated auxiliary forks');

$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_aux_map
SELECT g, repeat('x', 1200) FROM generate_series(1, 5000) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
});
cmp_ok($node->safe_psql('postgres',
		q{SELECT pg_relation_size('umbra_aux_map', 'fsm')}), '>', 0,
	'FSM fork was created');
cmp_ok($node->safe_psql('postgres',
		q{SELECT pg_relation_size('umbra_aux_map', 'vm')}), '>', 0,
	'VM fork was created');

# Model a crash where physical auxiliary forks survive but their root
# activation did not. Redo must activate mapping even for an existing file.
$node->stop('immediate');
overwrite_binary_file($map_path, $root_without_aux);
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '5000',
	'redo recovers data after restoring a root without auxiliary activation');
check_aux_layout($node, $main_path, $block_size, 'after auxiliary redo birth', 1);

# Persist active frontiers, generate new auxiliary redo, and remove both
# physical forks. Recovery must materialize their declared mapped capacity.
$node->safe_psql('postgres', 'CHECKPOINT');
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
check_aux_layout($node, $main_path, $block_size,
	'after missing-fork redo before maintenance', 1);
$node->safe_psql('postgres', 'VACUUM (FREEZE, ANALYZE) umbra_aux_map');
my $before_truncate =
	check_aux_layout($node, $main_path, $block_size,
		'after missing-fork recovery', 1);

# FSM and VM intentionally read with RBM_ZERO_ON_ERROR. Their normal
# maintenance callers must repair a corrupt mapped physical page.
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop;
corrupt_first_page($fsm_path);
corrupt_first_page($vm_path);
$node->start;
is($node->safe_psql('postgres',
		q{INSERT INTO umbra_aux_map VALUES (6000, repeat('z', 1200)); SELECT count(*) FROM umbra_aux_map;}),
	'5501', 'FSM zero-on-error path tolerates a corrupt mapped page');
$node->safe_psql('postgres', 'VACUUM (FREEZE, ANALYZE) umbra_aux_map');
pass('VM zero-on-error path tolerates a corrupt mapped page');

# Leave a checkpointed pre-truncate image, then retain only truncate WAL.
# Restoring MAIN and _map after the immediate stop models a crash before the
# physical truncation, while removing the auxiliary files forces truncate redo
# to use their root declarations before any auxiliary page redo can recreate
# them.
$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_aux_map;
VACUUM (FREEZE, ANALYZE, TRUNCATE FALSE) umbra_aux_map;
CHECKPOINT;
});
my $pre_truncate =
	check_aux_layout($node, $main_path, $block_size,
		'checkpointed pre-truncate state', 0);
my $main_before_truncate = slurp_file($node->data_dir . "/$main_path");
my $map_before_truncate = slurp_file($map_path);
my $fsm_before_truncate = slurp_file($fsm_path);
my $vm_before_truncate = slurp_file($vm_path);
cmp_ok($pre_truncate->{vm_eof}, '>', 0,
	'checkpointed pre-truncate VM still has a logical page');

$node->safe_psql(
	'postgres', q{
VACUUM (TRUNCATE, DISABLE_PAGE_SKIPPING) umbra_aux_map;
});

$node->stop('immediate');
my $control_path = $node->data_dir . '/global/pg_control';
my $control_before_truncate_recovery = slurp_file($control_path);
overwrite_binary_file($node->data_dir . "/$main_path", $main_before_truncate);
overwrite_binary_file($map_path, $map_before_truncate);
unlink $fsm_path or BAIL_OUT("could not remove \"$fsm_path\": $!");
unlink $vm_path or BAIL_OUT("could not remove \"$vm_path\": $!");
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '0',
	'auxiliary truncate redo survives missing-fork recovery');
my $after_truncate_recovery =
	check_aux_layout($node, $main_path, $block_size,
		'after auxiliary truncate recovery', 0);
cmp_ok($after_truncate_recovery->{fsm_eof}, '<=', $pre_truncate->{fsm_eof},
	'FSM logical EOF does not grow across truncate redo');
cmp_ok($after_truncate_recovery->{vm_eof}, '<', $pre_truncate->{vm_eof},
	'VM logical EOF is lowered by truncate redo');
cmp_ok($after_truncate_recovery->{fsm_capacity}, '<=',
	$pre_truncate->{fsm_capacity},
	'FSM capacity does not grow across truncate redo');
cmp_ok($after_truncate_recovery->{vm_capacity}, '<',
	$pre_truncate->{vm_capacity},
	'VM capacity is lowered by truncate redo');

# Model a recovery crash after the lower auxiliary root was persisted but
# before the old physical tails were removed.  The repeated truncate redo must
# still enter Umbra's prepare callback even when the generic FSM/VM helpers see
# that the logical fork sizes are already at their targets.
$node->stop('immediate');
overwrite_binary_file($fsm_path, $fsm_before_truncate);
overwrite_binary_file($vm_path, $vm_before_truncate);
overwrite_binary_file($control_path, $control_before_truncate_recovery);
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'), '0',
	'repeated truncate recovery preserves the empty relation');
check_aux_layout($node, $main_path, $block_size,
	'after repeated truncate recovery cleans stale auxiliary tails', 0);

$node->stop;
done_testing();
