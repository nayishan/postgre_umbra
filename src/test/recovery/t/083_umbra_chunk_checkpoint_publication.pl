# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the Exp2 C3 checkpoint-crossing schedule for chunk-paired Umbra:
# pause after block-shift WAL insertion and before MAP publication, prove a
# concurrent CHECKPOINT waits on the checkpoint-start barrier, then release and
# verify crash recovery starts from the completed checkpoint with correct data.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub relation_signature_sql
{
	return q[
SELECT count(*) || '|' ||
       sum(id)::bigint || '|' ||
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
FROM umb_c3_t;
];
}

sub wait_for_marker_file
{
	my ($path, $timeout_secs) = @_;

	for (my $i = 0; $i < $timeout_secs * 100; $i++)
	{
		return 1 if -e $path;
		select(undef, undef, undef, 0.01);
	}

	return 0;
}

my $node = PostgreSQL::Test::Cluster->new('umbra_chunk_checkpoint_publication');
my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);

$node->init();
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
full_page_writes = on
shared_buffers = '256MB'
max_wal_size = '4GB'
min_wal_size = '1GB'
checkpoint_timeout = '1h'
log_checkpoints = on
]);
$node->start();

$node->safe_psql('postgres', q[
CREATE TABLE umb_c3_t(id int, payload text) WITH (fillfactor = 70);
ALTER TABLE umb_c3_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umb_c3_t
SELECT g, repeat('x', 7000)
FROM generate_series(1, 180) AS g;
CHECKPOINT;
]);

my $pause_marker = $node->data_dir . '/umbra_exp2_c3_pause_entered';
my $release_marker = $node->data_dir . '/umbra_exp2_c3_release';
unlink $pause_marker;
unlink $release_marker;

my $update = $node->background_psql('postgres');
$update->{stdin} .= q[
SET umbra_exp2_c3_pause = on;
UPDATE umb_c3_t
SET payload = md5(id::text) || repeat('u', 6968)
WHERE split_part(trim(both '()' from ctid::text), ',', 1)::int
      IN (0, 31, 32, 63, 64);
\echo update_done
];
$update->{run}->pump_nb();

ok(wait_for_marker_file($pause_marker, 30),
	'update paused after block-shift WAL insert and before MAP publication');

my $checkpoint = $node->background_psql('postgres');
$checkpoint->{stdin} .= "CHECKPOINT;\n\\echo checkpoint_done\n";
$checkpoint->{run}->pump_nb();

ok($node->poll_query_until(
		'postgres',
		q[
SELECT EXISTS (
  SELECT 1
  FROM pg_stat_activity
  WHERE backend_type = 'checkpointer'
    AND wait_event = 'CheckpointDelayStart'
);
],
		't'),
	'CHECKPOINT waits on the checkpoint-start barrier');

$checkpoint->{run}->pump_nb();
unlike($checkpoint->{stdout}, qr/checkpoint_done/,
	'CHECKPOINT does not complete while MAP publication is paused');

open(my $release_fh, '>', $release_marker)
  or die "could not create $release_marker: $!";
close($release_fh) or die "could not close $release_marker: $!";

ok(pump_until(
		$update->{run},
		$psql_timeout,
		\$update->{stdout},
		qr/update_done/),
	'update finishes after releasing C3 pause');

my $signature_after_update =
  $node->safe_psql('postgres', relation_signature_sql());

ok(pump_until(
		$checkpoint->{run},
		$psql_timeout,
		\$checkpoint->{stdout},
		qr/checkpoint_done/),
	'CHECKPOINT completes after MAP publication');

$update->quit;
$checkpoint->quit;

$node->stop('immediate');
$node->start();

is($node->safe_psql('postgres', relation_signature_sql()),
	$signature_after_update,
	'checkpoint-visible MAP publication preserves relation contents after crash restart');

is($node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM umb_c3_t
		   WHERE left(payload, 8) = left(md5(id::text), 8);]),
	'5',
	'updated rows are visible after checkpoint-crossing crash recovery');

$node->stop();

done_testing();
