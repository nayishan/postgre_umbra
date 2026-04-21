# Verify FSM/VM forks participate in UMBRA MAP translation.
#
# In UMBRA mode this test checks that FSM and VM logical block 0 both get a
# valid mapping entry in relation MAP fork (entry != 0xFFFFFFFF).
#
# In md mode, MAP fork does not exist and the test is skipped.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

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

# Under the current proportional MAP layout:
#   block 0 = superblock
#   block 1 = FSM map page 0
#   block 2 = VM map page 0
my $fsm_map_page = 1;
my $vm_map_page = 2;

my $fsm_map_entry = $node->safe_psql(
	'postgres', qq{
SELECT COALESCE(
	encode(
		pg_read_binary_file(
			pg_relation_filepath('umb_fsm_vm_t') || '_map',
			current_setting('block_size')::int * $fsm_map_page,
			4,
			true),
		'hex'),
	'');
});

my $vm_map_entry = $node->safe_psql(
	'postgres', qq{
SELECT COALESCE(
	encode(
		pg_read_binary_file(
			pg_relation_filepath('umb_fsm_vm_t') || '_map',
			current_setting('block_size')::int * $vm_map_page,
			4,
			true),
		'hex'),
	'');
});

isnt($fsm_map_entry, '', 'FSM map entry is readable');
isnt($vm_map_entry, '', 'VM map entry is readable');
isnt($fsm_map_entry, 'ffffffff', 'FSM block 0 has a valid map entry');
isnt($vm_map_entry, 'ffffffff', 'VM block 0 has a valid map entry');

$node->stop('immediate');
$node->start();

my $fsm_map_entry_after_restart = $node->safe_psql(
	'postgres', qq{
SELECT COALESCE(
	encode(
		pg_read_binary_file(
			pg_relation_filepath('umb_fsm_vm_t') || '_map',
			current_setting('block_size')::int * $fsm_map_page,
			4,
			true),
		'hex'),
	'');
});

my $vm_map_entry_after_restart = $node->safe_psql(
	'postgres', qq{
SELECT COALESCE(
	encode(
		pg_read_binary_file(
			pg_relation_filepath('umb_fsm_vm_t') || '_map',
			current_setting('block_size')::int * $vm_map_page,
			4,
			true),
		'hex'),
	'');
});

isnt($fsm_map_entry_after_restart, 'ffffffff',
	'FSM block 0 map entry survives restart');
isnt($vm_map_entry_after_restart, 'ffffffff',
	'VM block 0 map entry survives restart');

done_testing();
