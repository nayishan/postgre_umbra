# Verify mapwriter process visibility and basic activity metadata.
#
# In UMBRA mode:
# - map writer backend should exist in pg_stat_activity
# - wait event should be MapwriterMain/MapwriterHibernate or NULL transiently
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

$node->safe_psql('postgres', q{CREATE TABLE umb_mapwriter_t(a int, b text);});

my $mapwriter_cnt = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'map writer';});
is($mapwriter_cnt, '1', 'map writer backend exists');

my $mapwriter_wait_ok = $node->safe_psql(
	'postgres', q{
SELECT count(*) > 0
FROM pg_stat_activity
WHERE backend_type = 'map writer'
  AND (wait_event IN ('MapwriterMain', 'MapwriterHibernate')
	   OR wait_event IS NULL);
});
is($mapwriter_wait_ok, 't', 'map writer wait event is expected');

# Create allocation pressure and ensure map writer remains visible.
$node->safe_psql(
	'postgres', q{
INSERT INTO umb_mapwriter_t
SELECT g, repeat('w', 300) FROM generate_series(1, 30000) g;
CHECKPOINT;
});

ok($node->poll_query_until('postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'map writer';},
	't'),
	'map writer remains alive under allocation pressure');

done_testing();
