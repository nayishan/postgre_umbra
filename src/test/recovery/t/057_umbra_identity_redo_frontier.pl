# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that redo repairs a missing identity MAP entry even when the
# crash-persisted superblock says that the logical block already exists.

use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_SET);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant MAP_FIRST_GROUP_BLOCK => 1;
use constant MAP_GROUP_MAIN_PAGES => 8192;
use constant MAP_GROUP_TOTAL_PAGES => 8194;
use constant MAP_SUPER_NEXT_FREE_MAIN_OFFSET => 16;
use constant MAP_SUPER_PHYS_CAPACITY_MAIN_OFFSET => 20;
use constant MAP_SUPER_LOGICAL_MAIN_OFFSET => 40;
use constant MAP_SUPER_CRC_OFFSET => 60;
use constant INVALID_BLOCK_NUMBER => 0xFFFFFFFF;

sub sql_literal
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub map_block_number
{
	my ($lblkno, $entries_per_page) = @_;
	my $fork_page = int($lblkno / $entries_per_page);
	my $group = int($fork_page / MAP_GROUP_MAIN_PAGES);

	return MAP_FIRST_GROUP_BLOCK + $group * MAP_GROUP_TOTAL_PAGES + 2
	  + ($fork_page % MAP_GROUP_MAIN_PAGES);
}

sub read_map_super
{
	my ($node, $map_path) = @_;
	my $hex = $node->safe_psql(
		'postgres',
		'SELECT encode(pg_read_binary_file('
		  . sql_literal($map_path)
		  . ", 0, 64, false), 'hex');");

	BAIL_OUT('short Umbra MAP superblock') unless length($hex) == 128;
	return pack('H*', $hex);
}

sub set_u32
{
	my ($buffer_ref, $offset, $value) = @_;
	substr(${$buffer_ref}, $offset, 4, pack('L', $value));
}

sub u32_at
{
	my ($buffer, $offset) = @_;
	return unpack('L', substr($buffer, $offset, 4));
}

sub write_at
{
	my ($fh, $offset, $data, $label) = @_;
	my $position = sysseek($fh, $offset, SEEK_SET);

	BAIL_OUT("could not seek to $label")
	  unless defined($position) && $position == $offset;
	my $nwritten = syswrite($fh, $data);
	BAIL_OUT("could not write $label")
	  unless defined($nwritten) && $nwritten == length($data);
}

my $node = PostgreSQL::Test::Cluster->new('identity_redo_frontier');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
fsync = on
});
$node->start;

my $block_size = 0 + $node->safe_psql(
	'postgres',
	q{SELECT pg_size_bytes(current_setting('block_size'));});
my $entries_per_page = int($block_size / 4);

$node->safe_psql(
	'postgres', q{
CREATE TABLE identity_redo_t(id int, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE identity_redo_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO identity_redo_t
SELECT g, repeat('x', 400) FROM generate_series(1, 2000) AS g;
CHECKPOINT;
});

my $old_nblocks = 0 + $node->safe_psql(
	'postgres',
	"SELECT pg_relation_size('identity_redo_t') / $block_size;");

$node->safe_psql(
	'postgres', q{
INSERT INTO identity_redo_t
SELECT g, repeat('y', 400) FROM generate_series(2001, 4000) AS g;
});

my $new_nblocks = 0 + $node->safe_psql(
	'postgres',
	"SELECT pg_relation_size('identity_redo_t') / $block_size;");
cmp_ok($new_nblocks, '>', $old_nblocks,
	'post-checkpoint insert extends the MAIN fork');

my $main_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('identity_redo_t'::regclass);});
my $map_path = "${main_path}_map";
my $super = read_map_super($node, $map_path);

# Model a crash that persisted the relation frontier but not every identity MAP
# entry.  Recovery must repair a requested missing entry as L -> L instead of
# treating the first MAP gap as EOF.
set_u32(\$super, MAP_SUPER_NEXT_FREE_MAIN_OFFSET, $new_nblocks);
set_u32(\$super, MAP_SUPER_PHYS_CAPACITY_MAIN_OFFSET, $new_nblocks);
set_u32(\$super, MAP_SUPER_LOGICAL_MAIN_OFFSET, $new_nblocks);
my $payload_hex = unpack('H*', substr($super, 0, MAP_SUPER_CRC_OFFSET));
my $crc = 0 + $node->safe_psql(
	'postgres',
	"SELECT crc32c(decode('$payload_hex', 'hex'));");
set_u32(\$super, MAP_SUPER_CRC_OFFSET, $crc);

my $target_lblkno = $old_nblocks;
my $map_block = map_block_number($target_lblkno, $entries_per_page);
my $map_entry_offset = $map_block * $block_size
  + ($target_lblkno % $entries_per_page) * 4;

$node->stop('immediate');

my $map_file = $node->data_dir . "/$map_path";
open(my $map_fh, '+<:raw', $map_file)
  or BAIL_OUT("could not open \"$map_file\": $!");
write_at($map_fh, 0, $super, 'Umbra MAP superblock');
write_at(
	$map_fh,
	$map_entry_offset,
	pack('L', INVALID_BLOCK_NUMBER),
	"Umbra MAP entry for logical block $target_lblkno");
close($map_fh) or BAIL_OUT("could not close \"$map_file\": $!");

$node->start;

is(
	$node->safe_psql(
		'postgres',
		q{SELECT count(*), min(id), max(id) FROM identity_redo_t;}),
	'4000|1|4000',
	'redo uses the superblock frontier and restores all rows');

my $entry_hex = $node->safe_psql(
	'postgres',
	'SELECT encode(pg_read_binary_file('
	  . sql_literal($map_path)
	  . ", $map_entry_offset, 4, false), 'hex');");
is(unpack('L', pack('H*', $entry_hex)), $target_lblkno,
	'redo repairs the missing mapping as logical block L to physical block L');

my $recovered_super = read_map_super($node, $map_path);
is(u32_at($recovered_super, MAP_SUPER_LOGICAL_MAIN_OFFSET), $new_nblocks,
	'recovery does not shrink the superblock logical EOF at a MAP gap');

$node->stop;
done_testing();
