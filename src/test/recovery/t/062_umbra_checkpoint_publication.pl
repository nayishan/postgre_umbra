# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that checkpoint start cannot cross the interval after an exact
# mapping WAL record is inserted and before its MAP state becomes visible.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('umbra_checkpoint_publication');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '4GB'
]);
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql('postgres', q[
CREATE TABLE umbra_checkpoint_publication(id integer);
CHECKPOINT;
]);
$node->safe_psql(
	'postgres',
	q[SELECT injection_points_attach(
         'umbra-mapping-after-wal-before-publish', 'wait')]);

my $insert = $node->background_psql('postgres');
$insert->query_until(
	qr/preload_started/,
	q(\echo preload_started
SELECT injection_points_run(
  'umbra-mapping-after-wal-before-publish');
\echo preload_done
));
$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
$node->safe_psql(
	'postgres',
	q[SELECT injection_points_wakeup(
	         'umbra-mapping-after-wal-before-publish')]);
$insert->query_until(qr/preload_done/, '');

$insert->query_until(
	qr/insert_started/,
	q(\echo insert_started
INSERT INTO umbra_checkpoint_publication VALUES (1);
\echo insert_done
));

$node->wait_for_event('client backend',
	'umbra-mapping-after-wal-before-publish');
pass('mapping WAL insertion pauses before MAP publication');

my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));

$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('checkpoint waits for MAP publication');

$node->safe_psql(
	'postgres',
	q[SELECT injection_points_wakeup(
         'umbra-mapping-after-wal-before-publish')]);
$insert->query_until(qr/insert_done/, '');
$checkpoint->query_until(qr/checkpoint_done/, '');

is($node->safe_psql('postgres',
		q[SELECT count(*) FROM umbra_checkpoint_publication]),
	'1', 'insert and checkpoint complete after MAP publication');

$node->safe_psql(
	'postgres',
	q[SELECT injection_points_detach(
         'umbra-mapping-after-wal-before-publish')]);
$insert->quit;
$checkpoint->quit;
$node->stop;

done_testing();
