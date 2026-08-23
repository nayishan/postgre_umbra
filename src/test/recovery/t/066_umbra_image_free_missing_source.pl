# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that unresolved image-free source dependencies fail crash recovery
# without creating, repairing, truncating, or resetting available storage.

use strict;
use warnings FATAL => 'all';

use Digest::SHA;
use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

sub file_sha256
{
	my ($path) = @_;
	open(my $fh, '<', $path) or BAIL_OUT("could not open \"$path\": $!");
	binmode($fh);
	my $digest = Digest::SHA->new(256);
	$digest->addfile($fh);
	close($fh) or BAIL_OUT("could not close \"$path\": $!");
	return $digest->hexdigest;
}

my $node = PostgreSQL::Test::Cluster->new('image_free_missing_source');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_flush_after = 1
checkpoint_timeout = '1h'
full_page_writes = on
log_min_messages = debug2
max_wal_size = '4GB'
restart_after_crash = off
]);
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql(
	'postgres', q[
CREATE TABLE image_free_missing_main(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO image_free_missing_main VALUES (1, 'initial');

CREATE TABLE image_free_old_high(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO image_free_old_high
SELECT g, repeat(md5(g::text), 4) FROM generate_series(1, 6000) AS g;

CREATE TABLE image_free_future_small(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO image_free_future_small VALUES (1, 'future');
CHECKPOINT;
]);

my $missing_relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('image_free_missing_main'::regclass);]);
my $old_high_relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('image_free_old_high'::regclass);]);
my $future_relpath = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('image_free_future_small'::regclass);]);
my $missing_filenode = 0 + $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('image_free_missing_main'::regclass);]);
my $old_high_filenode = 0 + $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('image_free_old_high'::regclass);]);
my $missing_block = 0 + $node->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM image_free_missing_main WHERE id = 1;]);
my ($old_high_id, $old_high_block) = split(/\|/, $node->safe_psql(
		'postgres', q[
SELECT id, (ctid::text::point)[0]::integer
FROM image_free_old_high
ORDER BY (ctid::text::point)[0]::integer DESC, id DESC
LIMIT 1;]));
cmp_ok($old_high_block, '>', 1,
	'old target is above the future relation logical EOF');

is(0 + $node->safe_psql(
		'postgres', q[
UPDATE image_free_missing_main SET payload = 'slot-one' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]),
	$missing_block, 'missing-MAIN first update remains on its logical block');
is(0 + $node->safe_psql(
		'postgres', qq[
UPDATE image_free_old_high SET payload = 'slot-one' WHERE id = $old_high_id
RETURNING (ctid::text::point)[0]::integer;]),
	$old_high_block, 'future-storage first update remains on its logical block');
my $first_updates_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_attach('umbra-checkpoint-after-buffer-selection', 'wait')]);
my $checkpoint = $node->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$node->wait_for_event('checkpointer', 'umbra-checkpoint-after-buffer-selection');
my $image_free_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

is(0 + $node->safe_psql(
		'postgres', q[
UPDATE image_free_missing_main SET payload = 'missing-image-free' WHERE id = 1
RETURNING (ctid::text::point)[0]::integer;]),
	$missing_block, 'missing-MAIN image-free update stays on its logical block');
is(0 + $node->safe_psql(
		'postgres', qq[
UPDATE image_free_old_high SET payload = 'future-image-free'
WHERE id = $old_high_id
RETURNING (ctid::text::point)[0]::integer;]),
	$old_high_block,
	'future-storage image-free update stays on its logical block');

$node->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-buffer-selection')]);
$checkpoint->query_until(qr/checkpoint_done/, '');
$node->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-buffer-selection')]);
$checkpoint->quit;

is($node->safe_psql(
		'postgres',
		"SELECT redo_lsn >= '$first_updates_end'::pg_lsn "
		  . "AND redo_lsn <= '$image_free_start'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'crash redo excludes the first FPI shifts but includes image-free WAL');

$node->stop('immediate');

my $missing_main_path = $node->data_dir . "/$missing_relpath";
my $missing_map_path = $node->data_dir . "/${missing_relpath}_map";
my $old_high_main_path = $node->data_dir . "/$old_high_relpath";
my $old_high_map_path = $node->data_dir . "/${old_high_relpath}_map";
my $future_main_path = $node->data_dir . "/$future_relpath";
my $future_map_path = $node->data_dir . "/${future_relpath}_map";

unlink($missing_main_path)
  or BAIL_OUT("could not remove \"$missing_main_path\": $!");
copy($future_main_path, $old_high_main_path)
  or BAIL_OUT("could not replace \"$old_high_main_path\": $!");
copy($future_map_path, $old_high_map_path)
  or BAIL_OUT("could not replace \"$old_high_map_path\": $!");
ok(!-e $missing_main_path,
	'missing MAIN injection occurs only after immediate stop');

my $missing_map_digest = file_sha256($missing_map_path);
my $future_main_digest = file_sha256($old_high_main_path);
my $future_map_digest = file_sha256($old_high_map_path);
my $failure_log_offset = -s $node->logfile;

ok(!$node->start(fail_ok => 1),
	'crash recovery rejects unresolved image-free source dependencies');
ok($node->log_contains(
		qr/page $missing_block of relation .*\/$missing_filenode does not exist/,
		$failure_log_offset),
	'missing MAIN is recorded as a logical invalid-page dependency');
ok($node->log_contains(
		qr/page $old_high_block of relation .*\/$old_high_filenode does not exist/,
		$failure_log_offset),
	'future storage below the old logical block is recorded as missing');
ok($node->log_contains(
		qr/WAL contains references to invalid pages/, $failure_log_offset),
	'unresolved dependencies are checked at crash-recovery completion');

ok(!-e $missing_main_path,
	'image-free probing does not recreate a missing MAIN file');
is(file_sha256($missing_map_path), $missing_map_digest,
	'image-free probing does not repair or reset the retained selector file');
is(file_sha256($old_high_main_path), $future_main_digest,
	'image-free probing does not truncate or reset newer MAIN storage');
is(file_sha256($old_high_map_path), $future_map_digest,
	'image-free probing does not rewrite newer MAP storage');

done_testing();
