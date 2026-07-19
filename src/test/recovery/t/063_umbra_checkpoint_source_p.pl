# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify checkpoint writeback to the physical mapping captured before remap.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');
plan skip_all => 'injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('checkpoint_source_p');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
});
$node->start;

plan skip_all => 'extension injection_points not installed'
  unless $node->check_extension('injection_points');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql('postgres', 'CREATE EXTENSION pg_buffercache');
$node->safe_psql(
	'postgres', q{
CREATE TABLE checkpoint_source_p(
  id integer primary key,
  before_checkpoint integer,
  after_map_flush integer,
  padding text)
  WITH (fillfactor = 50, autovacuum_enabled = false);
INSERT INTO checkpoint_source_p
VALUES (1, 0, 0, repeat('x', 1000));
SELECT padding FROM checkpoint_source_p WHERE id = 1;
CHECKPOINT;
});
my $filenode = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filenode('checkpoint_source_p')});

# Clear shared buffers so the writer's read opens the physical fork used by
# both remaps below.
$node->restart;
my $writer = $node->background_psql('postgres');
$writer->query_safe('SELECT padding FROM checkpoint_source_p WHERE id = 1');
$writer->query_safe(
	'UPDATE checkpoint_source_p SET before_checkpoint = 1 WHERE id = 1');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-map', 'wait')});
my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/starting_checkpoint/,
	q(
\echo starting_checkpoint
CHECKPOINT;
\echo checkpoint_done
\q
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-map');
pass('checkpoint pauses after MAP flush and before data-buffer writeback');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
my $target_block = $writer->query_safe(q{
UPDATE checkpoint_source_p SET after_map_flush = 2 WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
$writer->quit;

# A non-checkpointer flush must satisfy the captured source P and then clean
# the current P so that callers such as eviction can rely on a clean buffer.
my $evicter = $node->background_psql('postgres');
my $buffer_id = $evicter->query_safe(qq{
SELECT bufferid
FROM pg_buffercache
WHERE relfilenode = $filenode
  AND reldatabase = (SELECT oid FROM pg_database
                     WHERE datname = current_database())
  AND relforknumber = 0
  AND relblocknumber = $target_block;
});
$evicter->query_safe('SELECT pg_stat_reset_backend_stats(pg_backend_pid())');
is(
	$evicter->query_safe(qq{
SELECT buffer_evicted::integer::text || '|' ||
       buffer_flushed::integer::text
FROM pg_buffercache_evict($buffer_id);
}),
	'1|1',
	'ordinary eviction writes the remapped buffer and makes it reusable');
$evicter->query_safe('SELECT pg_stat_force_next_flush()');
is(
	$evicter->query_safe(q{
SELECT COALESCE(sum(writes), 0)
FROM pg_stat_get_backend_io(pg_backend_pid())
WHERE context = 'normal' AND object = 'relation';
}),
	'2',
	'ordinary eviction writes both checkpoint source P and current P');
$evicter->quit;

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-after-map')});
$checkpoint->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-map')});

my ($wal_dump, $wal_dump_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $wal_start, '--end', $wal_end
	]);
is(normalize_waldump_stderr($wal_dump_stderr), '',
	'pg_waldump reads the concurrent update WAL');
my @target_remap_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk \d+/ &&
	/; remap: lblkno \d+ old_pblkno \d+ new_pblkno \d+/
} split(/\n/, $wal_dump);
cmp_ok(scalar(@target_remap_records), '>=', 1,
	'update after MAP flush uses old-P remap WAL');
ok(!grep(/\bFPW\b/, @target_remap_records),
	'checkpoint-overlap remap is image-free');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres', q{
SELECT before_checkpoint::text || '|' || after_map_flush
FROM checkpoint_source_p WHERE id = 1;
}),
	'1|2',
	'crash recovery preserves both sides of the checkpoint boundary');

# Remove the earlier HOT chain so that pruning cannot consume the first-write
# image before the update whose remap this scenario verifies.
$node->safe_psql('postgres', 'VACUUM (FREEZE) checkpoint_source_p');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->restart;

# Persist the latest mapping before replay begins at a checkpoint-overlap
# remap.  Recovery must not reject the on-disk MAP because it already names
# the WAL record's new P.
my $chain_writer = $node->background_psql('postgres');
$chain_writer->query_safe(
	'SELECT padding FROM checkpoint_source_p WHERE id = 1');
$chain_writer->query_safe(
	'UPDATE checkpoint_source_p SET before_checkpoint = 3 WHERE id = 1');
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-before-map', 'wait')});
my $chain_checkpoint = $node->background_psql('postgres');
$chain_checkpoint->query_until(
	qr/starting_chain_checkpoint/,
	q(
\echo starting_chain_checkpoint
CHECKPOINT;
\echo chain_checkpoint_done
\q
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-before-map');
pass('second checkpoint pauses after buffer selection and before MAP flush');

my $chain_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
$chain_writer->query_safe(
	'UPDATE checkpoint_source_p SET after_map_flush = 5 WHERE id = 1');
my $chain_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
$chain_writer->quit;

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-before-map')});
$chain_checkpoint->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-before-map')});

my ($chain_wal, $chain_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $chain_wal_start, '--end', $chain_wal_end
	]);
is(normalize_waldump_stderr($chain_wal_stderr), '',
	'pg_waldump reads the remap chain WAL');
my @chain_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk \d+/ &&
	/; remap: lblkno \d+ old_pblkno \d+ new_pblkno \d+/
} split(/\n/, $chain_wal);
cmp_ok(scalar(@chain_records), '>=', 1,
	'checkpoint-overlap WAL contains the remap step');
ok(!grep(/\bFPW\b/, @chain_records),
	'checkpoint-overlap remap is image-free');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres', q{
SELECT before_checkpoint::text || '|' || after_map_flush
FROM checkpoint_source_p WHERE id = 1;
}),
	'3|5',
	'recovery rebuilds a remap when disk MAP already names new P');

# Leave one dirty page for C1.  The first remap is then published while C1 is
# paused after its MAP flush, and an ordinary eviction waits just before it
# examines C1's checkpoint state.
$node->safe_psql('postgres', 'VACUUM (FREEZE) checkpoint_source_p');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->restart;

my $epoch_writer = $node->background_psql('postgres');
$epoch_writer->query_safe(
	'SELECT padding FROM checkpoint_source_p WHERE id = 1');
$epoch_writer->query_safe(
	'UPDATE checkpoint_source_p SET before_checkpoint = 6 WHERE id = 1');
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-map', 'wait')});
my $epoch_checkpoint_1 = $node->background_psql('postgres');
$epoch_checkpoint_1->query_until(
	qr/starting_epoch_checkpoint_1/,
	q(
\echo starting_epoch_checkpoint_1
CHECKPOINT;
\echo epoch_checkpoint_1_done
\q
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-map');
pass('C1 pauses after publishing its MAP checkpoint');

my $epoch_target_block = $epoch_writer->query_safe(q{
UPDATE checkpoint_source_p SET after_map_flush = 7 WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});
my $epoch_evicter = $node->background_psql('postgres');
my $epoch_buffer_id = $epoch_evicter->query_safe(qq{
SELECT bufferid
FROM pg_buffercache
WHERE relfilenode = $filenode
  AND reldatabase = (SELECT oid FROM pg_database
                     WHERE datname = current_database())
  AND relforknumber = 0
  AND relblocknumber = $epoch_target_block;
});
$epoch_evicter->query_safe(q{
SELECT injection_points_set_local();
SELECT injection_points_attach(
  'umbra-buffer-flush-before-checkpoint-state', 'wait');
SELECT injection_points_load(
  'umbra-buffer-flush-before-checkpoint-state');
SELECT pg_stat_reset_backend_stats(pg_backend_pid());
});
$epoch_evicter->query_until(
	qr/starting_epoch_eviction/,
	qq(
\\echo starting_epoch_eviction
CREATE TEMP TABLE epoch_eviction_result AS
SELECT buffer_evicted, buffer_flushed
FROM pg_buffercache_evict($epoch_buffer_id);
\\echo epoch_eviction_done
));
$node->wait_for_event(
	'client backend', 'umbra-buffer-flush-before-checkpoint-state');
pass('ordinary eviction pauses before reading C1 checkpoint state');

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-after-map')});
$epoch_checkpoint_1->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-map')});
pass('C1 completes while the ordinary eviction remains paused');

# C2 captures P1 for the same dirty buffer.  Its first update remaps P1 to
# P2; the waiting eviction must observe C2 rather than stale C1 state.
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-map', 'wait')});
my $epoch_checkpoint_2 = $node->background_psql('postgres');
$epoch_checkpoint_2->query_until(
	qr/starting_epoch_checkpoint_2/,
	q(
\echo starting_epoch_checkpoint_2
CHECKPOINT;
\echo epoch_checkpoint_2_done
\q
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-map');
pass('C2 pauses after publishing its MAP checkpoint');

my $epoch_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
my $epoch_second_target_block = $epoch_writer->query_safe(q{
UPDATE checkpoint_source_p SET before_checkpoint = 8 WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});
my $epoch_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is($epoch_second_target_block, $epoch_target_block,
	'both checkpoint epochs remap the same logical block');
$epoch_writer->quit;

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup(
  'umbra-buffer-flush-before-checkpoint-state')});
$epoch_evicter->query_until(qr/epoch_eviction_done/, '');
is(
	$epoch_evicter->query_safe(q{
SELECT buffer_evicted::integer::text || '|' ||
       buffer_flushed::integer::text
FROM epoch_eviction_result;
}),
	'1|1',
	'cross-epoch eviction writes the remapped buffer and makes it reusable');
$epoch_evicter->query_safe('SELECT pg_stat_force_next_flush()');
is(
	$epoch_evicter->query_safe(q{
SELECT COALESCE(sum(writes), 0)
FROM pg_stat_get_backend_io(pg_backend_pid())
WHERE context = 'normal' AND object = 'relation';
}),
	'2',
	'cross-epoch eviction writes both C2 source P and current P');
$epoch_evicter->quit;

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_wakeup('umbra-checkpoint-after-map')});
$epoch_checkpoint_2->{run}->finish;
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-map')});

my ($epoch_wal, $epoch_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $epoch_wal_start, '--end', $epoch_wal_end
	]);
is(normalize_waldump_stderr($epoch_wal_stderr), '',
	'pg_waldump reads the C2 remap WAL');
my @epoch_remap_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk $epoch_target_block/ &&
	/; remap: lblkno $epoch_target_block old_pblkno \d+ new_pblkno \d+/
} split(/\n/, $epoch_wal);
is(scalar(@epoch_remap_records), 1,
	'C2 emits one remap record for the target block');
ok(!grep(/\bFPW\b/, @epoch_remap_records),
	'C2 remap is image-free');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres', q{
SELECT before_checkpoint::text || '|' || after_map_flush
FROM checkpoint_source_p WHERE id = 1;
}),
	'8|7',
	'crash recovery preserves the cross-epoch remap chain');

# A failed checkpoint advances the current redo pointer, but cannot make its
# source P a durable baseline.  The next update must therefore retain an FPI.
$node->safe_psql('postgres', 'VACUUM (FREEZE) checkpoint_source_p');
$node->safe_psql('postgres', 'CHECKPOINT');
my $failed_writer = $node->background_psql('postgres');
$failed_writer->query_safe(
	'SELECT padding FROM checkpoint_source_p WHERE id = 1');
my $failed_target_block = $failed_writer->query_safe(q{
UPDATE checkpoint_source_p SET after_map_flush = 9 WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});

$node->safe_psql(
	'postgres',
	q{SELECT injection_points_attach('umbra-checkpoint-after-map', 'error')});
my ($failed_checkpoint_rc, $failed_checkpoint_stdout,
	$failed_checkpoint_stderr) = $node->psql('postgres', 'CHECKPOINT');
isnt($failed_checkpoint_rc, 0,
	'checkpoint fails after advancing its current redo pointer');
like($failed_checkpoint_stderr, qr/checkpoint request failed/,
	'client observes the injected checkpoint failure');
$node->safe_psql(
	'postgres',
	q{SELECT injection_points_detach('umbra-checkpoint-after-map')});

my $failed_wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
my $failed_second_target_block = $failed_writer->query_safe(q{
UPDATE checkpoint_source_p SET before_checkpoint = 10 WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;
});
my $failed_wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is($failed_second_target_block, $failed_target_block,
	'both failed-checkpoint updates modify the same logical block');
$failed_writer->quit;

my ($failed_wal, $failed_wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $failed_wal_start, '--end', $failed_wal_end
	]);
is(normalize_waldump_stderr($failed_wal_stderr), '',
	'pg_waldump reads the post-failure update WAL');
my @failed_target_records = grep {
	/rel \d+\/\d+\/$filenode fork main blk $failed_target_block/
} split(/\n/, $failed_wal);
cmp_ok(scalar(@failed_target_records), '>=', 1,
	'post-failure WAL contains the target relation block');
ok(grep(/\bFPW\b/, @failed_target_records),
	'post-failure update retains a full-page image');
ok(!grep(/; remap: lblkno $failed_target_block old_pblkno \d+ new_pblkno \d+/,
		@failed_target_records),
	'post-failure update does not use an existing-P remap baseline');

$node->stop('immediate');
$node->start;
is(
	$node->safe_psql(
		'postgres', q{
SELECT before_checkpoint::text || '|' || after_map_flush
FROM checkpoint_source_p WHERE id = 1;
}),
	'10|9',
	'crash recovery preserves both failed-checkpoint updates');

$node->stop;
done_testing();
