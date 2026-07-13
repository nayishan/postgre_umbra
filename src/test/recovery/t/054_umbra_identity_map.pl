# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify Umbra identity MAP entries and private-fork lifecycle.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

# Patch 4 layout: [superblock][FSM page][VM page][8192 MAIN pages].
use constant MAP_FIRST_GROUP_BLOCK => 1;
use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_GROUP_TOTAL_PAGES => 8194;

sub sql_literal
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub map_block_number
{
	my ($fork, $fork_page) = @_;

	return MAP_FIRST_GROUP_BLOCK + $fork_page * MAP_GROUP_TOTAL_PAGES
	  if $fork eq 'fsm';
	return MAP_FIRST_GROUP_BLOCK + $fork_page * MAP_GROUP_TOTAL_PAGES + 1
	  if $fork eq 'vm';

	if ($fork eq 'main')
	{
		my $group = int($fork_page / MAP_GROUP_MAIN_PAGES);
		return MAP_FIRST_GROUP_BLOCK + $group * MAP_GROUP_TOTAL_PAGES + 2
		  + ($fork_page % MAP_GROUP_MAIN_PAGES);
	}

	die "unknown fork $fork";
}

sub read_map_page
{
	my ($node, $map_path, $block_size, $map_block) = @_;
	my $offset = $map_block * $block_size;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($map_path)
		  . ", $offset, $block_size, false), 'hex');");

	die "short MAP page $map_block"
	  unless length($hex) == 2 * $block_size;

	# BlockNumber is a native-endian uint32, as is Perl's L template.
	my @entries = unpack('L*', pack('H*', $hex));
	return \@entries;
}

sub read_fork_map
{
	my ($node, $map_path, $block_size, $entries_per_page,
		$fork, $nblocks) = @_;

	die "$fork fork is empty" if $nblocks <= 0;

	my @mapped;
	my $last_fork_page = int(($nblocks - 1) / $entries_per_page);

	for my $fork_page (0 .. $last_fork_page)
	{
		my $map_block = map_block_number($fork, $fork_page);
		my $page =
		  read_map_page($node, $map_path, $block_size, $map_block);
		my $first = $fork_page * $entries_per_page;
		my $take = $nblocks - $first;

		$take = $entries_per_page if $take > $entries_per_page;
		push @mapped, @{$page}[0 .. $take - 1];
	}

	return \@mapped;
}

sub relation_blocks
{
	my ($node, $fork, $block_size) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('umb_identity_t', '$fork')"
		  . " / $block_size;");
}

sub check_identity_maps
{
	my ($node, $map_path, $block_size, $entries_per_page,
		$nblocks, $phase) = @_;

	my %mapped;

	for my $fork (qw(main fsm vm))
	{
		$mapped{$fork} = read_fork_map(
			$node, $map_path, $block_size, $entries_per_page,
			$fork, $nblocks->{$fork});

		my @identity = (0 .. $nblocks->{$fork} - 1);
		is_deeply(
			$mapped{$fork},
			\@identity,
			"$phase: every $fork MAP entry is identity");
	}

	is(
		$mapped{main}->[$entries_per_page - 1],
		$entries_per_page - 1,
		"$phase: last entry of first MAIN MAP page is identity");
	is(
		$mapped{main}->[$entries_per_page],
		$entries_per_page,
		"$phase: first entry of second MAIN MAP page is identity");
}

my $node = PostgreSQL::Test::Cluster->new('identity_map');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
fsync = on
});
$node->start;

my $block_size = 0 + $node->safe_psql(
	'postgres',
	q{SELECT pg_size_bytes(current_setting('block_size'));});
my $entries_per_page = int($block_size / 4);
my $row_count = $entries_per_page + 8;
my $payload_repeats = int($block_size / 64);

is(
	$node->safe_psql(
		'postgres', q{
CREATE TEMP TABLE umb_identity_temp(id int);
INSERT INTO umb_identity_temp VALUES (1), (2);
SELECT count(*)::text || '|' ||
       ((pg_stat_file(pg_relation_filepath('umb_identity_temp') || '_map',
                      true)).size IS NULL)::text
FROM umb_identity_temp;
}),
	'2|true',
	'temporary relation uses direct identity access without a MAP fork');

$node->safe_psql(
	'postgres', q{
CREATE UNLOGGED TABLE umb_identity_unlogged(id int);
INSERT INTO umb_identity_unlogged VALUES (1), (2);
});
my $unlogged_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_identity_unlogged');});
is(
	$node->safe_psql(
		'postgres',
		'SELECT (pg_stat_file('
		  . sql_literal("${unlogged_path}_map")
		  . ', true)).size IS NULL;'),
	't',
	'unlogged relation uses direct identity access without a MAP fork');

$node->safe_psql(
	'postgres', q{
CREATE TABLE umb_identity_t(id int, payload text);
ALTER TABLE umb_identity_t ALTER COLUMN payload SET STORAGE PLAIN;
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_identity_t');});
my $map_path = "${main_path}_map";

is(
	$node->safe_psql(
		'postgres',
		'SELECT (pg_stat_file('
		  . sql_literal($map_path)
		  . ', true)).size IS NOT NULL;'),
	't',
	'creating MAIN creates the private _map fork');
is(
	0 + $node->safe_psql(
		'postgres',
		'SELECT COALESCE((pg_stat_file('
		  . sql_literal($map_path)
		  . ', true)).size, -1);'),
	$block_size,
	'new identity MAP fork contains one superblock page');

# A PLAIN tuple slightly larger than half a page guarantees one heap tuple
# per page.  This crosses the first MAIN MAP page with about 16MB on an 8KB
# build.
$node->safe_psql(
	'postgres', qq{
INSERT INTO umb_identity_t
SELECT g, repeat(md5(g::text), $payload_repeats)
FROM generate_series(1, $row_count) AS g;
});

$node->safe_psql(
	'postgres', q{
VACUUM (FREEZE, ANALYZE) umb_identity_t;
CHECKPOINT;
});

my %nblocks = map {
	$_ => relation_blocks($node, $_, $block_size)
} qw(main fsm vm);

is($nblocks{main}, $row_count,
	'one PLAIN half-page tuple occupies each MAIN block');
cmp_ok($nblocks{main}, '>', $entries_per_page,
	'MAIN crosses its first MAP page');
cmp_ok($nblocks{fsm}, '>', 0, 'FSM fork exists');
cmp_ok($nblocks{vm}, '>', 0, 'VM fork exists');

my $map_size = 0 + $node->safe_psql(
	'postgres',
	'SELECT (pg_stat_file('
	  . sql_literal($map_path)
	  . ', false)).size;');
cmp_ok($map_size / $block_size, '>=', 5,
	'MAP contains its superblock, FSM, VM, and two MAIN map pages');

check_identity_maps(
	$node, $map_path, $block_size, $entries_per_page,
	\%nblocks, 'before restart');

$node->stop('immediate');
$node->start;

is(
	$node->safe_psql('postgres', 'SELECT count(*) FROM umb_identity_unlogged;'),
	'0',
	'unlogged relation is reset after an immediate restart');
is(
	$node->safe_psql(
		'postgres',
		'SELECT (pg_stat_file('
		  . sql_literal("${unlogged_path}_map")
		  . ', true)).size IS NULL;'),
	't',
	'unlogged relation remains direct after restart');

is(
	$node->safe_psql(
		'postgres',
		qq{
SELECT count(*), min(id), max(id),
       bool_and(octet_length(payload) = $block_size / 2)
FROM umb_identity_t;
}),
	"$row_count|1|$row_count|t",
	'all heap data is readable after checkpoint and immediate restart');

my %after_restart = map {
	$_ => relation_blocks($node, $_, $block_size)
} qw(main fsm vm);

is_deeply(\%after_restart, \%nblocks,
	'tracked fork sizes survive immediate restart');
check_identity_maps(
	$node, $map_path, $block_size, $entries_per_page,
	\%after_restart, 'after restart');

$node->safe_psql('postgres', 'DROP TABLE umb_identity_t;');
$node->safe_psql('postgres', 'DROP TABLE umb_identity_unlogged;');

is(
	$node->safe_psql(
		'postgres',
		'SELECT (pg_stat_file('
		  . sql_literal($map_path)
		  . ', true)).size IS NULL;'),
	't',
	'dropping MAIN immediately removes its private MAP fork');

$node->safe_psql('postgres', 'CHECKPOINT');

is(
	$node->safe_psql(
		'postgres',
		'SELECT (pg_stat_file('
		  . sql_literal($map_path)
		  . ', true)).size IS NULL;'),
	't',
	'dropped MAP remains absent after checkpoint');

$node->stop;
done_testing();
