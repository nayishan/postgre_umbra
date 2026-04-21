# Verify UMBRA delayed unlink behavior for MAIN fork segment 0.
#
# In UMBRA mode for permanent relations:
# - DROP first truncates MAIN seg0 to 0 bytes
# - actual unlink of MAIN seg0 is delayed to checkpoint
#
# In md mode, skip this test.
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
CREATE TABLE umb_head_unlink_t(id int, payload text);
INSERT INTO umb_head_unlink_t
SELECT g, repeat('z', 2000) FROM generate_series(1, 15000) g;
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_head_unlink_t');}
);

cmp_ok(
	$node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1);"),
	'>',
	0,
	'MAIN seg0 size is non-zero before DROP');

$node->safe_psql('postgres', q{DROP TABLE umb_head_unlink_t;});

ok($node->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) = 0;"),
	'MAIN seg0 is truncated to 0 before checkpoint (delayed unlink stage)');

$node->safe_psql('postgres', q{CHECKPOINT;});

ok($node->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) = -1;"),
	'MAIN seg0 is physically removed after checkpoint');

done_testing();
