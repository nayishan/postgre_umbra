# Verify FSM/VM forks participate in Umbra chunk-paired translation.
#
# In UMBRA mode this test checks that FSM and VM on-disk metadata is recorded
# in the relation metadata superblock.  Base-born pages use the missing/all-zero
# active-slot default and therefore do not require materialized active-slot pages.
#
# In md mode, MAP fork does not exist and the test is skipped.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub u32le_from_hex
{
	my ($hex, $offset) = @_;
	my $chunk = substr($hex, $offset * 2, 8);
	my @b = ($chunk =~ /../g);

	return hex($b[0]) +
	  (hex($b[1]) << 8) +
	  (hex($b[2]) << 16) +
	  (hex($b[3]) << 24);
}

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
});
$node->start();

$node->safe_psql(
	'postgres', q{
CREATE TABLE umb_fsm_vm_t(id int, payload text);
INSERT INTO umb_fsm_vm_t
SELECT g, repeat('x', 2000) FROM generate_series(1, 15000) g;
VACUUM (FREEZE, ANALYZE) umb_fsm_vm_t;
});

my $fsm_size = $node->safe_psql(
	'postgres', q{
SELECT pg_relation_size('umb_fsm_vm_t', 'fsm');
});
my $vm_size = $node->safe_psql(
	'postgres', q{
SELECT pg_relation_size('umb_fsm_vm_t', 'vm');
});

cmp_ok($fsm_size, '>', 0, 'FSM fork has at least one page');
cmp_ok($vm_size, '>', 0, 'VM fork has at least one page');

$node->safe_psql('postgres', 'CHECKPOINT;');

my $map_super_hex = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_fsm_vm_t') || '_map', 0, 64, true), 'hex');}
);
my $logical_fsm = u32le_from_hex($map_super_hex, 44);
my $logical_vm = u32le_from_hex($map_super_hex, 48);

cmp_ok($logical_fsm, '>', 0, 'FSM logical EOF is tracked in metadata');
cmp_ok($logical_vm, '>', 0, 'VM logical EOF is tracked in metadata');

$node->stop('immediate');
$node->start();

my $map_super_hex_after_restart = $node->safe_psql(
	'postgres',
	q{SELECT encode(pg_read_binary_file(pg_relation_filepath('umb_fsm_vm_t') || '_map', 0, 64, true), 'hex');}
);
my $logical_fsm_after_restart = u32le_from_hex($map_super_hex_after_restart, 44);
my $logical_vm_after_restart = u32le_from_hex($map_super_hex_after_restart, 48);

is($logical_fsm_after_restart, $logical_fsm,
	'FSM logical EOF survives restart');
is($logical_vm_after_restart, $logical_vm, 'VM logical EOF survives restart');

done_testing();
