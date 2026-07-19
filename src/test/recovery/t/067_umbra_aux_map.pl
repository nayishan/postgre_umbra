# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify append-only FSM/VM allocation and exact auxiliary mapping redo.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant INVALID_BLOCK_NUMBER => 0xFFFFFFFF;
use constant MAP_LOGICAL_FSM_OFFSET => 20;
use constant MAP_LOGICAL_VM_OFFSET => 24;
use constant MAP_PHYSICAL_FSM_OFFSET => 32;
use constant MAP_PHYSICAL_VM_OFFSET => 36;
use constant MAP_FIRST_GROUP_BLOCK => 1;
use constant MAP_GROUP_FSM_PAGES => 1;
use constant MAP_GROUP_VM_PAGES => 1;
use constant MAP_GROUP_MAIN_PAGES => 8192;

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

my $node = PostgreSQL::Test::Cluster->new('umbra_aux_map');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
fsync = on
map_prealloc_main_low = 1073741823
});
$node->start;

my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'));});
my $payload_repeats = int($block_size / 64);

$node->safe_psql(
	'postgres', q{
CREATE TABLE umbra_aux_map(id integer, payload text)
  WITH (autovacuum_enabled = false, vacuum_truncate = true);
ALTER TABLE umbra_aux_map ALTER COLUMN payload SET STORAGE PLAIN;
});
$node->safe_psql(
	'postgres', qq{
INSERT INTO umbra_aux_map
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, 256) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
CHECKPOINT;
});

my $relpath = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('umbra_aux_map');});
my $data_path = $node->data_dir . "/$relpath";
my $map_path = "${data_path}_map";
my %logical_offset = (
	fsm => MAP_LOGICAL_FSM_OFFSET,
	vm => MAP_LOGICAL_VM_OFFSET);
my %frontier_offset = (
	fsm => MAP_PHYSICAL_FSM_OFFSET,
	vm => MAP_PHYSICAL_VM_OFFSET);
my (%initial_logical, %old_frontier, %old_file_blocks);

for my $fork (qw(fsm vm))
{
	$initial_logical{$fork} = read_u32($map_path, $logical_offset{$fork});
	cmp_ok($initial_logical{$fork}, '>', 0,
		"initial $fork fork is populated");
	$old_frontier{$fork} = read_u32($map_path, $frontier_offset{$fork});
	$old_file_blocks{$fork} = (-s "${data_path}_$fork") / $block_size;
	isnt($old_frontier{$fork}, INVALID_BLOCK_NUMBER,
		"initial $fork physical frontier is valid");
	is(read_mapping($map_path, $block_size, $fork, 0), 0,
		"initial $fork block zero is identity");
}

# VACUUM truncates all three forks without replacing the relfilenode.  The
# following load therefore reuses each auxiliary fork's truncated logical EOF
# while retaining its old physical frontier.
$node->safe_psql(
	'postgres', q{
DELETE FROM umbra_aux_map;
VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) umbra_aux_map;
});
is($node->safe_psql(
		'postgres', q{SELECT pg_relation_filepath('umbra_aux_map');}),
	$relpath, 'VACUUM truncate keeps the same relation storage');
$node->safe_psql('postgres', 'CHECKPOINT');
my %truncated_logical;
for my $fork (qw(fsm vm))
{
	$truncated_logical{$fork} =
	  read_u32($map_path, $logical_offset{$fork});
	cmp_ok($truncated_logical{$fork}, '<', $initial_logical{$fork},
		"VACUUM truncate reduces the logical $fork EOF");
	is(read_u32($map_path, $frontier_offset{$fork}), $old_frontier{$fork},
		"VACUUM truncate retains the physical $fork frontier");
	is((-s "${data_path}_$fork") / $block_size, $old_file_blocks{$fork},
		"VACUUM truncate retains the physical $fork file");
	for my $lblkno ($truncated_logical{$fork} .. $initial_logical{$fork} - 1)
	{
		is(read_mapping($map_path, $block_size, $fork, $lblkno),
			INVALID_BLOCK_NUMBER,
			"VACUUM truncate clears $fork logical block $lblkno");
	}
	isnt($truncated_logical{$fork}, $old_frontier{$fork},
		"$fork re-extension will use a non-identity physical block");
}

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', qq{
INSERT INTO umbra_aux_map
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1001, 1256) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_map;
});
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

my ($wal, $wal_stderr) = run_command(
	[
		'pg_waldump', '--bkp-details',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $wal_start,
		'--end' => $wal_end,
	]);
is($wal_stderr, '', 'pg_waldump reads auxiliary mapping WAL');
for my $fork (qw(fsm vm))
{
	like($wal,
		qr/rmgr: Storage[^\n]*desc: UMBRA_MAP_EXTEND \Q$relpath\E_$fork logical $truncated_logical{$fork}\.\.[0-9]+ physical $old_frontier{$fork}\.\.[0-9]+/,
		"$fork re-extension records its exact non-identity mapping");
}

# No checkpoint has persisted the new MAP entries.  Recovery must first
# materialize each exact auxiliary P range and publish it at record end.
$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_map'),
	'256', 'heap remains readable after auxiliary mapping redo');
$node->safe_psql('postgres', 'CHECKPOINT');

for my $fork (qw(fsm vm))
{
	my $mapping = read_mapping($map_path, $block_size, $fork,
		$truncated_logical{$fork});

	cmp_ok(read_u32($map_path, $logical_offset{$fork}), '>',
		$truncated_logical{$fork},
		"recovery restores the logical $fork EOF");
	is($mapping, $old_frontier{$fork},
		"recovered $fork first reused block uses the reserved physical frontier");
	isnt($mapping, $truncated_logical{$fork},
		"recovered $fork mapping is L != P");
	cmp_ok((-s "${data_path}_$fork") / $block_size, '>',
		$old_file_blocks{$fork},
		"recovered $fork file covers the appended physical block");
}

# RelationCopyStorage writes each destination fork in batches.  Auxiliary fork
# FPIs cannot publish first-born mappings themselves, so the structural mapping
# WAL must precede the FPI that references the new logical page.
$node->safe_psql(
	'postgres', qq{
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE umbra_aux_copy_ts LOCATION '';
CREATE TABLE umbra_aux_copy(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE umbra_aux_copy ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umbra_aux_copy
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, 256) AS g;
VACUUM (FREEZE, ANALYZE) umbra_aux_copy;
CHECKPOINT;
});

$wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres',
	'ALTER TABLE umbra_aux_copy SET TABLESPACE umbra_aux_copy_ts;');
$wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

my ($copy_relpath, $copy_locator) = split(/\|/, $node->safe_psql(
	'postgres', q{
SELECT pg_relation_filepath('umbra_aux_copy'),
       reltablespace || '/' || (SELECT oid FROM pg_database
                                WHERE datname = current_database()) || '/' ||
       pg_relation_filenode('umbra_aux_copy')
FROM pg_class
WHERE oid = 'umbra_aux_copy'::regclass;
}));
($wal, $wal_stderr) = run_command(
	[
		'pg_waldump', '--bkp-details',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $wal_start,
		'--end' => $wal_end,
	]);
is($wal_stderr, '', 'pg_waldump reads RelationCopyStorage WAL');

my $copy_create_pos = index($wal, "desc: CREATE ${copy_relpath}_vm");
my $copy_extend_pos =
  index($wal, "desc: UMBRA_MAP_EXTEND ${copy_relpath}_vm");
my $copy_fpi_pos = -1;
if ($wal =~ /rmgr: XLOG[^\n]*desc: FPI \n\tblkref #0: rel \Q$copy_locator\E fork vm blk 0/)
{
	$copy_fpi_pos = $-[0];
}
cmp_ok($copy_create_pos, '>=', 0,
	'RelationCopyStorage WAL creates the destination VM fork');
cmp_ok($copy_extend_pos, '>', $copy_create_pos,
	'RelationCopyStorage publishes the VM mapping after create');
cmp_ok($copy_fpi_pos, '>', $copy_extend_pos,
	'RelationCopyStorage publishes the VM mapping before its FPI');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM umbra_aux_copy'),
	'256', 'RelationCopyStorage auxiliary mapping survives recovery');
$node->safe_psql(
	'postgres', q{
DROP TABLE umbra_aux_copy;
DROP TABLESPACE umbra_aux_copy_ts;
});

$node->stop;
done_testing();
