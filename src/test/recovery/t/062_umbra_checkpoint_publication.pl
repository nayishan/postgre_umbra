# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that checkpoint start cannot cross WAL-to-MAP publication.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');
plan skip_all => 'injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('checkpoint_publication');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = on
log_checkpoints = on
mapwriter_lru_maxpages = 0
mapwriter_prealloc_max_relations = 0
});
$node->start;

plan skip_all => 'extension injection_points not installed'
  unless $node->check_extension('injection_points');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql(
	'postgres', q{
CREATE TABLE checkpoint_publication(id integer primary key, payload text)
  WITH (fillfactor = 50, autovacuum_enabled = false);
INSERT INTO checkpoint_publication VALUES (1, repeat('a', 1000));
SELECT payload FROM checkpoint_publication WHERE id = 1;
CHECKPOINT;
});
my $relation_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('checkpoint_publication')});
my $filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('checkpoint_publication')});
my $physical_path = $node->data_dir . '/' . $relation_path;

# The same backend that updates the page must first open its physical fork.
$node->restart;
my $physical_size_before = -s $physical_path;
my $writer = $node->background_psql('postgres');
$writer->query_safe('SELECT payload FROM checkpoint_publication WHERE id = 1');
$writer->query_safe(q{
SELECT injection_points_attach('umbra-remap-after-wal', 'wait');
SELECT injection_points_load('umbra-remap-after-wal');
});
$writer->query_until(
	qr/starting_update/,
	q(
\echo starting_update
UPDATE checkpoint_publication SET payload = repeat('b', 1000) WHERE id = 1;
\echo update_done
\q
));

$node->wait_for_event('client backend', 'umbra-remap-after-wal');
pass('existing-page remap pauses after WAL and before MAP publication');
is(-s $physical_path, $physical_size_before,
	'WAL assembly reserves P without extending the physical file');

my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/starting_checkpoint/,
	q(
\echo starting_checkpoint
CHECKPOINT;
\echo checkpoint_done
\q
));
$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('checkpoint waits for remap publication');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-remap-after-wal')});
$writer->{run}->finish;
$checkpoint->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-remap-after-wal')});

is(
	$node->safe_psql(
		'postgres',
		q{SELECT payload = repeat('b', 1000) FROM checkpoint_publication}),
	't',
	'remap and checkpoint both complete after publication resumes');

# Start the remap only after the checkpoint's first delay snapshot, with buffer
# capture already published.  The fresh snapshot in CheckPointGuts() must catch
# it.
$node->restart;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-capture', 'wait')});
my $late_checkpoint = $node->background_psql('postgres');
$late_checkpoint->query_until(
	qr/starting_late_checkpoint/,
	q(
\echo starting_late_checkpoint
CHECKPOINT;
\echo late_checkpoint_done
\q
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-capture');
pass('checkpoint reaches its fresh delay snapshot after capture publication');

my $late_writer = $node->background_psql('postgres');
$late_writer->query_safe(
	'SELECT payload FROM checkpoint_publication WHERE id = 1');
$late_writer->query_safe(q{
SELECT injection_points_attach('umbra-remap-after-wal', 'wait');
SELECT injection_points_load('umbra-remap-after-wal');
});
$late_writer->query_until(
	qr/starting_late_update/,
	q(
\echo starting_late_update
UPDATE checkpoint_publication SET payload = repeat('c', 1000) WHERE id = 1;
\echo late_update_done
\q
));
$node->wait_for_event('client backend', 'umbra-remap-after-wal');
pass('late remap pauses after WAL and before MAP publication');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-after-capture')});
$node->wait_for_event('checkpointer', 'CheckpointDelayStart');
pass('fresh delay snapshot waits for the late remap');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-remap-after-wal')});
$late_writer->{run}->finish;
$late_checkpoint->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-capture')});
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-remap-after-wal')});

is(
	$node->safe_psql(
		'postgres',
		q{SELECT payload = repeat('c', 1000) FROM checkpoint_publication}),
	't',
	'late remap and checkpoint both complete after publication resumes');

# Make the heap page older than the current redo pointer, then pause the next
# checkpoint after capture is active but before its new redo record is visible.
$node->safe_psql(
	'postgres', 'VACUUM (FREEZE) checkpoint_publication; CHECKPOINT;');
$node->restart;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-capture-before-redo', 'wait')});
my $early_checkpoint = $node->background_psql('postgres');
$early_checkpoint->query_until(
	qr/starting_early_checkpoint/,
	q(
\echo starting_early_checkpoint
CHECKPOINT;
\echo early_checkpoint_done
\q
));
$node->wait_for_event(
	'checkpointer', 'umbra-checkpoint-after-capture-before-redo');
pass('checkpoint publishes capture before exposing its new redo pointer');

my $early_writer = $node->background_psql('postgres');
$early_writer->query_safe(
	'SELECT payload FROM checkpoint_publication WHERE id = 1');
my $early_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
my $early_target_block = $early_writer->query_safe(q{
UPDATE checkpoint_publication SET payload = repeat('d', 1000) WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});
my $early_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
$early_writer->quit;

my ($early_wal, $early_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $early_wal_start, '--end', $early_wal_end
	]);
$early_wal_stderr =~
  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
is($early_wal_stderr, '', 'pg_waldump reads the early-capture update WAL');
my @early_remap_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk $early_target_block/ &&
	/; remap: lblkno $early_target_block old_pblkno \d+ new_pblkno \d+/
} split(/\n/, $early_wal);
cmp_ok(scalar(@early_remap_records), '>=', 1,
	'early-capture update emits remap WAL for the target block');
ok(!grep(/\bFPW\b/, @early_remap_records),
	'early-capture remap is image-free');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-after-capture-before-redo')});
$early_checkpoint->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-capture-before-redo')});

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres',
		q{SELECT payload = repeat('d', 1000) FROM checkpoint_publication}),
	't',
	'crash recovery preserves the update across the early capture boundary');

$node->stop;
done_testing();
