# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that the first MAIN-fork hint update after a checkpoint uses a byte
# delta with chunk shift metadata, and that both crash recovery and the
# checkpoint-start interlock preserve the recorded source slot semantics.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub normalize_waldump_stderr
{
	my ($stderr) = @_;

	$stderr =~
	  s/^pg_waldump: first record is after [^\n]+, at [^\n]+, skipping over \d+ bytes?\n?//m;
	return $stderr;
}

sub xmin_is_committed
{
	my ($node, $relation) = @_;

	return $node->safe_psql(
		'postgres', qq{
SELECT EXISTS (
  SELECT 1
  FROM heap_page_items(get_raw_page('$relation', 0)) AS item
  CROSS JOIN LATERAL
    heap_tuple_infomask_flags(item.t_infomask, item.t_infomask2) AS flags
  WHERE item.lp = 1
    AND 'HEAP_XMIN_COMMITTED' = ANY (flags.raw_flags)
);});
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

my $node = PostgreSQL::Test::Cluster->new('umbra_chunk_hint_delta');
my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);

$node->init(extra => ['--data-checksums']);
$node->append_conf(
	'postgresql.conf', q[
wal_level = 'replica'
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
wal_log_hints = off
shared_buffers = '16MB'
]);
$node->start();

plan skip_all => 'extension pageinspect not installed'
	unless $node->check_extension('pageinspect');
$node->safe_psql('postgres', 'CREATE EXTENSION pageinspect');

is($node->safe_psql('postgres', 'SHOW data_checksums'), 'on',
	'checksums trigger the protective hint WAL path');

$node->safe_psql(
	'postgres', q[
CREATE TABLE umb_chunk_hint_delta(id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umb_chunk_hint_delta VALUES (1, 'before');
CHECKPOINT;
]);

my $filenode = $node->safe_psql(
	'postgres', q[SELECT pg_relation_filenode('umb_chunk_hint_delta')]);

is(xmin_is_committed($node, 'umb_chunk_hint_delta'), 'f',
	'checkpointed tuple does not yet have a committed hint bit');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn()');
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umb_chunk_hint_delta WHERE id = 1]),
	'before', 'first visibility check reads the tuple');

my $updated_block = $node->safe_psql(
	'postgres', q[
UPDATE umb_chunk_hint_delta SET payload = 'after' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]);
is($updated_block, '0', 'ordinary update remains on logical block zero');

my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_flush_lsn()');
my ($wal, $wal_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->data_dir . '/pg_wal',
		'--start', $wal_start, '--end', $wal_end
	]);
is(normalize_waldump_stderr($wal_stderr), '',
	'pg_waldump reads the hint and ordinary update WAL chain');

my @wal_records = split(/(?=^rmgr: )/m, $wal);
my @hint_records = grep {
	/rmgr: XLOG2/ && /desc: HINT_DELTA/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
is(scalar(@hint_records), 1,
	'first committed hint emits one HINT_DELTA record');
my $hint_record = $hint_records[0] // '';
like($hint_record, qr/desc: HINT_DELTA fragments 1, final bytes [12]\b/,
	'HINT_DELTA carries only the changed heap hint bytes');
unlike($hint_record, qr/\bFPW\b/,
	'HINT_DELTA does not carry a full-page image');
like($hint_record,
	qr/; shift: source_slot [0-2] target_slot [0-2] logical_nblocks 1/,
	'HINT_DELTA carries a distinct chunk source and target slot');

my @old_hint_fpi = grep {
	/desc: FPI_FOR_HINT/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
is(scalar(@old_hint_fpi), 0,
	'first committed hint does not emit FPI_FOR_HINT');

my @ordinary_updates = grep {
	/rmgr: Heap/ && /desc: (?:HOT_)?UPDATE/ &&
	/rel \d+\/\d+\/$filenode fork main blk 0/
} @wal_records;
cmp_ok(scalar(@ordinary_updates), '>=', 1,
	'ordinary update follows HINT_DELTA on the same logical page');
ok(!grep(/\bFPW\b/, @ordinary_updates),
	'ordinary update does not carry a full-page image');
ok(!grep(/; shift:/, @ordinary_updates),
	'ordinary update reuses the mapping published by HINT_DELTA');

$node->stop('immediate');
$node->start();

is(xmin_is_committed($node, 'umb_chunk_hint_delta'), 't',
	'crash recovery restores the committed hint bit from HINT_DELTA');
is($node->safe_psql(
		'postgres', q[SELECT payload FROM umb_chunk_hint_delta WHERE id = 1]),
	'after', 'ordinary WAL replays after the hint shift');

$node->safe_psql(
	'postgres', q[
CREATE TABLE umb_chunk_hint_c3(id integer, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umb_chunk_hint_c3 VALUES (1, 'checkpoint');
CHECKPOINT;
]);
is(xmin_is_committed($node, 'umb_chunk_hint_c3'), 'f',
	'C3 fixture starts without a committed hint bit');

my $pause_marker = $node->data_dir . '/umbra_exp2_c3_pause_entered';
my $release_marker = $node->data_dir . '/umbra_exp2_c3_release';
unlink $pause_marker;
unlink $release_marker;

my $selector = $node->background_psql('postgres');
$selector->{stdin} .= q[
SET umbra_exp2_c3_pause = on;
SELECT payload FROM umb_chunk_hint_c3 WHERE id = 1;
\echo hint_done
];
$selector->{run}->pump_nb();

ok(wait_for_marker_file($pause_marker, 30),
	'hint delta pauses after WAL insertion and before slot publication');

my $checkpoint = $node->background_psql('postgres');
$checkpoint->{stdin} .= "CHECKPOINT;\n\\echo checkpoint_done\n";
$checkpoint->{run}->pump_nb();

ok($node->poll_query_until(
		'postgres', q[
SELECT EXISTS (
  SELECT 1
  FROM pg_stat_activity
  WHERE backend_type = 'checkpointer'
    AND wait_event = 'CheckpointDelayStart'
);
], 't'),
	'CHECKPOINT waits for the hint delta checkpoint-start interlock');

open(my $release_fh, '>', $release_marker)
  or die "could not create $release_marker: $!";
close($release_fh) or die "could not close $release_marker: $!";

ok(pump_until(
		$selector->{run},
		$psql_timeout,
		\$selector->{stdout},
		qr/hint_done/),
	'hint query finishes after C3 release');
ok(pump_until(
		$checkpoint->{run},
		$psql_timeout,
		\$checkpoint->{stdout},
		qr/checkpoint_done/),
	'CHECKPOINT completes after hint slot publication');

$selector->quit;
$checkpoint->quit;

$node->stop('immediate');
$node->start();
is(xmin_is_committed($node, 'umb_chunk_hint_c3'), 't',
	'checkpoint-crossing crash recovery restores the hint delta');

$node->stop();

done_testing();
