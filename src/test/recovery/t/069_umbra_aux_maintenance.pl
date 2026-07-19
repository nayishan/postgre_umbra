# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify FSM/VM preallocation and cold shared-buffer FSM relocation.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

use constant INVALID_BLOCK_NUMBER => 0xFFFFFFFF;
use constant MAP_LOGICAL_FSM_OFFSET => 20;
use constant MAP_FIRST_GROUP_BLOCK => 1;
use constant MAP_GROUP_FSM_PAGES => 1;
use constant MAP_GROUP_VM_PAGES => 1;
use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_COMPACTOR_EXTENT_BLOCKS => 128;

sub read_u32
{
	my ($path, $offset) = @_;
	my $buffer;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, 4);
	defined($nread) && $nread == 4
	  or BAIL_OUT("could not read uint32 from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
	return unpack('L', $buffer);
}

sub read_mapping
{
	my ($path, $block_size, $fork, $lblkno) = @_;
	my $entries_per_page = $block_size / 4;
	my $fork_page = int($lblkno / $entries_per_page);
	my $group_pages = MAP_GROUP_FSM_PAGES + MAP_GROUP_VM_PAGES +
	  MAP_GROUP_MAIN_PAGES;
	my $map_block = MAP_FIRST_GROUP_BLOCK + $fork_page * $group_pages;

	$map_block += MAP_GROUP_FSM_PAGES if $fork eq 'vm';
	return read_u32($path,
		$map_block * $block_size + ($lblkno % $entries_per_page) * 4);
}

sub read_physical_block
{
	my ($path, $block_size, $pblkno) = @_;
	my $buffer;
	my $offset = $pblkno * $block_size;

	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nread = sysread($fh, $buffer, $block_size);
	defined($nread) && $nread == $block_size
	  or BAIL_OUT("could not read physical block $pblkno from \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
	return $buffer;
}

sub write_physical_block
{
	my ($path, $block_size, $pblkno, $buffer) = @_;
	my $offset = $pblkno * $block_size;

	length($buffer) == $block_size
	  or BAIL_OUT("invalid replacement block size for \"$path\"");
	open(my $fh, '+<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $position = sysseek($fh, $offset, SEEK_SET);
	defined($position) && $position == $offset
	  or BAIL_OUT("could not seek in \"$path\": $!");
	my $nwritten = syswrite($fh, $buffer);
	defined($nwritten) && $nwritten == $block_size
	  or BAIL_OUT("could not write physical block $pblkno to \"$path\"");
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
}

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_aux_maintenance');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
map_compactor_enable = off
map_compactor_extent_blocks = 128
map_compactor_low_live_percent = 100
map_compactor_max_moves = 1
mapcompactor_delay = '10ms'
mapcompactor_max_relations = 10000
map_prealloc_main_low = 1073741823
map_prealloc_fsm_low = 1073741823
map_prealloc_vm_low = 1073741823
mapwriter_delay = '10ms'
mapwriter_prealloc_max_relations = 10000
});
$node->start;

is($node->safe_psql('postgres', q{
SELECT string_agg(name || '=' || boot_val, ',' ORDER BY name)
FROM pg_settings
WHERE name IN ('map_prealloc_fsm_batch', 'map_prealloc_fsm_hard',
               'map_prealloc_fsm_low', 'map_prealloc_vm_batch',
               'map_prealloc_vm_hard', 'map_prealloc_vm_low');
}),
	'map_prealloc_fsm_batch=128,map_prealloc_fsm_hard=16,map_prealloc_fsm_low=64,' .
	'map_prealloc_vm_batch=128,map_prealloc_vm_hard=16,map_prealloc_vm_low=64',
	'FSM and VM preallocation defaults are independent');

my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'))});
$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_aux_maintenance(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_aux_maintenance ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_aux_maintenance VALUES (1, repeat('x', 100));
});
$node->safe_psql(
	'postgres', q{VACUUM (FREEZE, ANALYZE) umbra_aux_maintenance});
$node->safe_psql('postgres', 'CHECKPOINT');

my $relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_aux_maintenance')});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
my %before;
for my $fork (qw(fsm vm))
{
	my $size = -s "${data_path}_$fork";
	defined($size) or BAIL_OUT("missing $fork fork");
	$before{$fork} = $size / $block_size;
	cmp_ok($before{$fork}, '>', 0, "$fork fork is populated");
}
cmp_ok($before{fsm}, '>', 1,
	'one-block heap has multiple FSM pages');
my $fsm_logical_nblocks = read_u32($map_path, MAP_LOGICAL_FSM_OFFSET);
cmp_ok($fsm_logical_nblocks, '>', 1,
	'FSM logical size can straddle a compaction extent');

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_prealloc_fsm_low = 1;
ALTER SYSTEM SET map_prealloc_fsm_hard = 1;
ALTER SYSTEM SET map_prealloc_fsm_batch = 128;
ALTER SYSTEM SET map_prealloc_vm_low = 1;
ALTER SYSTEM SET map_prealloc_vm_hard = 1;
ALTER SYSTEM SET map_prealloc_vm_batch = 128;
SELECT pg_reload_conf();
});

my %preallocated;
for my $fork (qw(fsm vm))
{
	my $blocks = $before{$fork};
	for (1 .. 400)
	{
		$blocks = (-s "${data_path}_$fork") / $block_size;
		last if $blocks >= $before{$fork} + 128;
		usleep(50_000);
	}
	cmp_ok($blocks, '>=', $before{$fork} + 128,
		"mapwriter preallocates the $fork fork");
	$preallocated{$fork} = $blocks;
}

# Clear every mapping while retaining the append-only physical frontier and
# the preallocated file.  The relfilenode remains unchanged.
$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_aux_maintenance;
VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umbra_aux_maintenance;
CHECKPOINT;
});
cmp_ok(read_u32($map_path, MAP_LOGICAL_FSM_OFFSET), '<',
	$fsm_logical_nblocks, 'VACUUM truncates the logical FSM');

$node->stop;

# Put the physical EOF just before an extent boundary, without modifying MAP.
# Root load is responsible for reconciling file growth before it can reserve a
# new P.  Rebuilding the same small FSM then produces mappings on both sides of
# the boundary while MAIN and VM each retain only one live extent.
my $extent_blocks = MAP_COMPACTOR_EXTENT_BLOCKS;
my $boundary = int(
	($preallocated{fsm} + $fsm_logical_nblocks + $extent_blocks - 1) /
	  $extent_blocks) * $extent_blocks;
my $injected_frontier = $boundary - ($fsm_logical_nblocks - 1);
cmp_ok($injected_frontier, '>', $preallocated{fsm},
	'injected FSM frontier advances beyond preallocation');
ok(truncate("${data_path}_fsm", $injected_frontier * $block_size),
	'injected sparse growth advances the FSM physical EOF');
is((-s "${data_path}_fsm") / $block_size, $injected_frontier,
	'FSM physical EOF reaches the injected frontier');

$node->start;
$node->safe_psql(
	'postgres', q{
INSERT INTO umbra_aux_maintenance VALUES (1, repeat('x', 100));
VACUUM (FREEZE, ANALYZE) umbra_aux_maintenance;
CHECKPOINT;
});
is(read_u32($map_path, MAP_LOGICAL_FSM_OFFSET), $fsm_logical_nblocks,
	'rebuilt FSM restores its logical size');

my @fsm_mappings = map {
	read_mapping($map_path, $block_size, 'fsm', $_)
} 0 .. $fsm_logical_nblocks - 1;
is(scalar(grep { $_ != INVALID_BLOCK_NUMBER } @fsm_mappings),
	$fsm_logical_nblocks, 'rebuilt FSM has a canonical mapping for every page');
my $highest_extent = 0;
for my $pblkno (@fsm_mappings)
{
	$highest_extent = int($pblkno / $extent_blocks)
	  if int($pblkno / $extent_blocks) > $highest_extent;
}
cmp_ok($highest_extent, '>', 0,
	'root reconciliation rebuilds FSM across an extent boundary');
my ($victim_lblkno) = grep {
	int($fsm_mappings[$_] / $extent_blocks) < $highest_extent
} 0 .. $fsm_logical_nblocks - 1;
ok(defined($victim_lblkno),
	'rebuilt FSM retains a live page below its highest extent');
BAIL_OUT('could not select a low-extent FSM compaction candidate')
  unless defined($victim_lblkno);
my $victim_old_pblkno = $fsm_mappings[$victim_lblkno];
my $victim_image = read_physical_block(
	"${data_path}_fsm", $block_size, $victim_old_pblkno);
ok($victim_image ne "\0" x $block_size,
	'candidate FSM page has initialized shared-buffer contents');

# Persist the setting without reloading it.  Compaction starts only after the
# clean restart has removed the candidate FSM page from shared buffers.
$node->safe_psql(
	'postgres', q{ALTER SYSTEM SET map_compactor_enable = on});
$node->stop;

# A compactor move preserves an existing image; it must not inherit FSM's
# zero-on-error repair policy and publish a synthesized replacement page.
my $log_offset = -s $node->logfile;
write_physical_block("${data_path}_fsm", $block_size, $victim_old_pblkno,
	"x" x $block_size);
$node->start;
is($node->safe_psql(
		'postgres', q{SELECT count(*) FROM umbra_aux_maintenance}),
	'1', 'MAIN access makes the root resident with a damaged cold FSM page');
$node->wait_for_log(
	qr/invalid page in block $victim_lblkno of relation/, $log_offset);
is(read_mapping($map_path, $block_size, 'fsm', $victim_lblkno),
	$victim_old_pblkno, 'failed cold FSM read does not publish a remap');
$node->stop;
write_physical_block("${data_path}_fsm", $block_size, $victim_old_pblkno,
	$victim_image);
$node->start;

is($node->safe_psql(
		'postgres', q{SELECT count(*) FROM umbra_aux_maintenance}),
	'1', 'MAIN access makes the relation root resident after FSM repair');
ok($node->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity
	  WHERE backend_type = 'map compactor'},
	't'), 'map compactor is active for auxiliary dispatch');

my $fsm_mapping_after = $victim_old_pblkno;
for (1 .. 800)
{
	$fsm_mapping_after = read_mapping(
		$map_path, $block_size, 'fsm', $victim_lblkno);
	last if $fsm_mapping_after != $victim_old_pblkno;
	usleep(50_000);
}
isnt($fsm_mapping_after, $victim_old_pblkno,
	'compactor relocates a cold FSM page through the shared buffer pool');
isnt($fsm_mapping_after, INVALID_BLOCK_NUMBER,
	'cold FSM relocation publishes a valid physical target');

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = off;
SELECT pg_reload_conf();
CHECKPOINT;
});
my $victim_final_pblkno = read_mapping(
	$map_path, $block_size, 'fsm', $victim_lblkno);
isnt($victim_final_pblkno, $victim_old_pblkno,
	'checkpoint persists the cold FSM remap');
my $relocated_image = read_physical_block(
	"${data_path}_fsm", $block_size, $victim_final_pblkno);
ok($relocated_image eq $victim_image,
	'cold FSM relocation preserves the complete shared-buffer page image');

$node->safe_psql(
	'postgres', q{VACUUM (ANALYZE) umbra_aux_maintenance});
is($node->safe_psql(
		'postgres', q{SELECT count(*) FROM umbra_aux_maintenance}),
	'1', 'relocated FSM remains usable by VACUUM');

$node->stop;
done_testing();
