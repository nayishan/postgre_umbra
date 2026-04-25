# Verify UMBRA MAP fork policy and drop lifecycle behavior.
#
# In UMBRA mode:
# - permanent relations should have MAP fork
# - unlogged/temp relations should not have MAP fork
# - dropped permanent relation's MAP fork should disappear after checkpoint
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

my $perm_map_exists = $node->safe_psql(
	'postgres', q{
CREATE TABLE umb_perm_t(a int);
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_perm_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

my $unlogged_map_exists = $node->safe_psql(
	'postgres', q{
CREATE UNLOGGED TABLE umb_unlogged_t(a int);
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_unlogged_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

my $temp_map_exists = $node->safe_psql(
	'postgres', q{
CREATE TEMP TABLE umb_temp_t(a int);
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_temp_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

is($perm_map_exists, 't', 'permanent relation has MAP fork');
is($unlogged_map_exists, 'f', 'unlogged relation has no MAP fork');
is($temp_map_exists, 'f', 'temp relation has no MAP fork');

my $perm_map_path =
  $node->safe_psql('postgres',
	q{SELECT pg_relation_filepath('umb_perm_t') || '_map';});

$node->safe_psql('postgres', q{
DROP TABLE umb_perm_t;
CHECKPOINT;
});

ok($node->poll_query_until('postgres',
	"SELECT COALESCE(encode(pg_read_binary_file('$perm_map_path', 0, 1, true), 'hex'), '') = '';", 't'),
	'dropped permanent relation MAP fork disappears after checkpoint');

done_testing();
