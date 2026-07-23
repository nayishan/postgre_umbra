# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that missing image-free sources are recovery dependencies resolved by
# later DROP or TRUNCATE WAL, including exact relation-locator reuse.

use strict;
use warnings FATAL => 'all';

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

sub sql_literal
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub assert_image_free_shift_wal
{
	my ($node, $targets, $start_lsn, $end_lsn) = @_;
	my ($dump, $stderr) = run_command(
		[
			'pg_waldump', '--bkp-details',
			'--path' => $node->data_dir . '/pg_wal',
			'--start' => $start_lsn,
			'--end' => $end_lsn,
		]);

	$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
	is($stderr, '', 'pg_waldump reads all image-free update WAL');
	for my $target (@$targets)
	{
		my @records = grep {
			/rel \d+\/\d+\/$target->{filenode} fork main blk $target->{block}\b/
		} split(/\n/, $dump);
		is(scalar(@records), 1,
			"$target->{name} emits one target-block WAL record");
		unlike($records[0], qr/\bFPW\b/,
			"$target->{name} shift is image-free");
		like($records[0], qr/captured_source/,
			"$target->{name} records its captured source slot");
	}
}

sub updated_block
{
	my ($node, $table, $id, $payload) = @_;
	return 0 + $node->safe_psql(
		'postgres',
		"UPDATE $table SET payload = " . sql_literal($payload)
		  . " WHERE id = $id RETURNING (ctid::text::point)[0]::integer;");
}

my $primary = PostgreSQL::Test::Cluster->new('image_free_primary');
$primary->init;
$primary->append_conf(
	'postgresql.conf', q[
allow_in_place_tablespaces = on
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_flush_after = 1
checkpoint_timeout = '1h'
full_page_writes = on
log_min_messages = debug2
max_wal_size = '4GB'
restart_after_crash = off
shared_buffers = '32MB'
wal_keep_size = '1GB'
]);
$primary->start;

if (!$primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

my $regress_shlib = $ENV{REGRESS_SHLIB};
BAIL_OUT('REGRESS_SHLIB is not set') unless defined $regress_shlib;
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$primary->safe_psql(
	'postgres',
	'CREATE FUNCTION regress_set_next_oid(oid) RETURNS oid AS '
	  . sql_literal($regress_shlib)
	  . ", 'regress_set_next_oid' LANGUAGE C STRICT;");

$primary->safe_psql(
	'postgres', q[
CREATE TABLESPACE umbra_image_free_ts LOCATION '';
CREATE TABLE image_free_drop(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO image_free_drop VALUES (1, 'initial');

CREATE TABLE image_free_truncate(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO image_free_truncate
SELECT g, repeat(md5(g::text), 4) FROM generate_series(1, 6000) AS g;

CREATE TABLE image_free_reuse_old(id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50)
  TABLESPACE umbra_image_free_ts;
INSERT INTO image_free_reuse_old
SELECT g, repeat(md5(g::text), 4) FROM generate_series(1, 6000) AS g;
CHECKPOINT;
]);

my $drop_filenode = 0 + $primary->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('image_free_drop'::regclass);]);
my $drop_relpath = $primary->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('image_free_drop'::regclass);]);
my $drop_block = 0 + $primary->safe_psql(
	'postgres', q[
SELECT (ctid::text::point)[0]::integer
FROM image_free_drop WHERE id = 1;]);

my $truncate_filenode = 0 + $primary->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('image_free_truncate'::regclass);]);
my ($truncate_id, $truncate_block) = split(/\|/, $primary->safe_psql(
		'postgres', q[
SELECT id, (ctid::text::point)[0]::integer
FROM image_free_truncate
ORDER BY (ctid::text::point)[0]::integer DESC, id DESC
LIMIT 1;]));

my $reuse_filenode = 0 + $primary->safe_psql(
	'postgres',
	q[SELECT pg_relation_filenode('image_free_reuse_old'::regclass);]);
my $reuse_relpath = $primary->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('image_free_reuse_old'::regclass);]);
my ($reuse_low_id, $reuse_low_block) = split(/\|/, $primary->safe_psql(
		'postgres', q[
SELECT id, (ctid::text::point)[0]::integer
FROM image_free_reuse_old
ORDER BY (ctid::text::point)[0]::integer, id
LIMIT 1;]));
my ($reuse_high_id, $reuse_high_block) = split(/\|/, $primary->safe_psql(
		'postgres', q[
SELECT id, (ctid::text::point)[0]::integer
FROM image_free_reuse_old
ORDER BY (ctid::text::point)[0]::integer DESC, id DESC
LIMIT 1;]));

is($drop_block, 0, 'DROP target is on logical block zero');
cmp_ok($truncate_block, '>', 1, 'TRUNCATE target is above a small future EOF');
is($reuse_low_block, 0, 'reuse target overlaps a future block zero');
cmp_ok($reuse_high_block, '>', 1,
	'exact-reuse target also has a block above the future EOF');

# Set initial tuple commit hints before the baseline checkpoint.  This keeps
# FPI_FOR_HINT from consuming the first post-checkpoint modification, so the
# first updates advance every target from slot 0 to slot 1.
$primary->safe_psql(
	'postgres', qq[
SELECT payload FROM image_free_drop WHERE id = 1;
SELECT payload FROM image_free_truncate WHERE id = $truncate_id;
SELECT payload FROM image_free_reuse_old WHERE id = $reuse_low_id;
SELECT payload FROM image_free_reuse_old WHERE id = $reuse_high_id;
]);
$primary->safe_psql('postgres', 'CHECKPOINT');

is(updated_block($primary, 'image_free_drop', 1, 'slot-one'),
	$drop_block, 'DROP target first update remains on its logical block');
is(updated_block($primary, 'image_free_truncate', $truncate_id, 'slot-one'),
	$truncate_block,
	'TRUNCATE target first update remains on its logical block');
is(updated_block($primary, 'image_free_reuse_old', $reuse_low_id, 'slot-one'),
	$reuse_low_block,
	'reuse overlap first update remains on its logical block');
is(updated_block($primary, 'image_free_reuse_old', $reuse_high_id, 'slot-one'),
	$reuse_high_block,
	'reuse high first update remains on its logical block');

# Set commit hints for the first-update tuples before C1 starts.  Otherwise the
# second updates can emit FPI_FOR_HINT records after the restartpoint boundary,
# making those records, rather than the image-free shifts, reconstruct the
# source pages during crash recovery.
$primary->safe_psql(
	'postgres', qq[
SELECT payload FROM image_free_drop WHERE id = 1;
SELECT payload FROM image_free_truncate WHERE id = $truncate_id;
SELECT payload FROM image_free_reuse_old WHERE id = $reuse_low_id;
SELECT payload FROM image_free_reuse_old WHERE id = $reuse_high_id;
]);
my $first_updates_end = $primary->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

$primary->safe_psql(
	'postgres', q[
SELECT injection_points_attach('umbra-checkpoint-after-map', 'wait')]);
my $checkpoint = $primary->background_psql('postgres');
$checkpoint->query_until(
	qr/checkpoint_started/,
	q(\echo checkpoint_started
CHECKPOINT;
\echo checkpoint_done
));
$primary->wait_for_event('checkpointer', 'umbra-checkpoint-after-map');
pass('C1 is blocked after persisting all slot-one selectors');

my $image_free_start = $primary->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
is(updated_block($primary, 'image_free_drop', 1, 'drop-image-free'),
	$drop_block, 'DROP image-free update remains on its logical block');
is(updated_block($primary, 'image_free_truncate', $truncate_id,
		'truncate-image-free'),
	$truncate_block,
	'TRUNCATE image-free update remains on its logical block');
is(updated_block($primary, 'image_free_reuse_old', $reuse_low_id,
		'reuse-low-image-free'),
	$reuse_low_block,
	'reuse overlap image-free update remains on its logical block');
is(updated_block($primary, 'image_free_reuse_old', $reuse_high_id,
		'reuse-high-image-free'),
	$reuse_high_block,
	'reuse high image-free update remains on its logical block');
my $image_free_end = $primary->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');

assert_image_free_shift_wal(
	$primary,
	[
		{name => 'DROP target', filenode => $drop_filenode,
			block => $drop_block},
		{name => 'TRUNCATE target', filenode => $truncate_filenode,
			block => $truncate_block},
		{name => 'reuse overlap target', filenode => $reuse_filenode,
			block => $reuse_low_block},
		{name => 'reuse high target', filenode => $reuse_filenode,
			block => $reuse_high_block},
	],
	$image_free_start,
	$image_free_end);

$primary->safe_psql(
	'postgres', q[
SELECT injection_points_wakeup('umbra-checkpoint-after-map')]);
$checkpoint->query_until(qr/checkpoint_done/, '');
$primary->safe_psql(
	'postgres', q[
SELECT injection_points_detach('umbra-checkpoint-after-map')]);
$checkpoint->quit;

my $crash_redo = $primary->safe_psql(
	'postgres', q[SELECT redo_lsn FROM pg_control_checkpoint();]);
is($primary->safe_psql(
		'postgres',
		"SELECT redo_lsn >= '$first_updates_end'::pg_lsn "
		  . "AND redo_lsn <= '$image_free_start'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'crash redo excludes the first FPI shifts but includes image-free WAL');

# Preserve C1's control file.  Later checkpoints establish the physical state
# needed by the test, but restoring this copy after the crash forces redo to
# begin before the image-free shifts and to replay those later checkpoints.
my $saved_control = $primary->backup_dir . '/pg_control.before_cleanup';
ok(copy($primary->data_dir . '/global/pg_control', $saved_control),
	'save the pre-cleanup crash-recovery start point');

# This checkpoint is intentionally after the unresolved image-free records and
# before the lifecycle WAL that clears them.  Replaying it must not publish a
# restartpoint while the invalid-page dependencies are still outstanding.
$primary->safe_psql('postgres', 'CHECKPOINT');
is($primary->safe_psql(
		'postgres',
		"SELECT redo_lsn >= '$image_free_end'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'dependency checkpoint follows the image-free shifts');

$primary->safe_psql(
	'postgres', 'DROP TABLE image_free_drop, image_free_reuse_old;');
$primary->safe_psql(
	'postgres', 'DELETE FROM image_free_truncate WHERE id <> 1;');
$primary->safe_psql(
	'postgres',
	'VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) image_free_truncate;');
cmp_ok(0 + $primary->safe_psql(
		'postgres',
		q[SELECT pg_relation_size('image_free_truncate') / current_setting('block_size')::integer;]),
	'<=', $truncate_block,
	'primary TRUNCATE removes the old image-free target block');

$primary->safe_psql('postgres', 'CHECKPOINT');
ok(!-e $primary->data_dir . "/$drop_relpath",
	'checkpoint removes the dropped MAIN file');
ok(!-e $primary->data_dir . "/${drop_relpath}_map",
	'checkpoint leaves no dropped MAP file');
ok(!-e $primary->data_dir . "/$reuse_relpath",
	'checkpoint removes the old reuse MAIN gate');
ok(!-e $primary->data_dir . "/${reuse_relpath}_map",
	'checkpoint leaves no old reuse MAP file');

$primary->safe_psql(
	'postgres', 'CREATE TABLE image_free_reuse_new(marker integer);');
$primary->safe_psql(
	'postgres',
	"SELECT regress_set_next_oid($reuse_filenode); "
	  . 'ALTER TABLE image_free_reuse_new SET TABLESPACE umbra_image_free_ts;');
is(0 + $primary->safe_psql(
		'postgres',
		q[SELECT pg_relation_filenode('image_free_reuse_new'::regclass);]),
	$reuse_filenode, 'new relation reuses the exact old filenode');
is($primary->safe_psql(
		'postgres',
		q[SELECT pg_relation_filepath('image_free_reuse_new'::regclass);]),
	$reuse_relpath, 'new relation reuses the exact old locator');
$primary->safe_psql(
	'postgres', 'INSERT INTO image_free_reuse_new VALUES (424242);');
is($primary->safe_psql(
		'postgres', q[SELECT marker FROM image_free_reuse_new;]),
	'424242', 'new locator stores its own marker WAL');

$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->stop('immediate');

ok(copy($saved_control, $primary->data_dir . '/global/pg_control'),
	'restore the pre-cleanup crash-recovery start point');
unlink($saved_control)
  or BAIL_OUT("could not remove \"$saved_control\": $!");

my ($crash_redo_hi, $crash_redo_lo) = split('/', $crash_redo);
my $crash_redo_log = sprintf('%X/%08X', hex($crash_redo_hi),
	hex($crash_redo_lo));
my $crash_log_offset = -s $primary->logfile;
$primary->start;
$primary->wait_for_log(
	qr/redo starts at \Q$crash_redo_log\E/, $crash_log_offset);

$primary->log_check(
	'image-free recovery dependency evidence', $crash_log_offset,
	log_like => [
		qr/page $drop_block of relation .*\/$drop_filenode does not exist/,
		qr/page $truncate_block of relation .*\/$truncate_filenode does not exist/,
		qr/page $reuse_low_block of relation .*\/$reuse_filenode is uninitialized/,
		qr/page $reuse_high_block of relation .*\/$reuse_filenode does not exist/,
		qr/could not record restart point at .* because there are unresolved references to invalid pages/,
	]);

is($primary->safe_psql(
		'postgres', q[SELECT to_regclass('image_free_drop') IS NULL;]),
	't', 'DROP clears its missing-source dependency');
is($primary->safe_psql(
		'postgres', q[SELECT count(*) FROM image_free_truncate;]),
	'1', 'covering TRUNCATE clears its high-block dependency');
is($primary->safe_psql(
		'postgres', q[SELECT marker FROM image_free_reuse_new;]),
	'424242', 'exact-reused relation remains readable after crash recovery');

$primary->safe_psql('postgres', 'CHECKPOINT');
is($primary->safe_psql(
		'postgres',
		"SELECT redo_lsn > '$crash_redo'::pg_lsn "
		  . 'FROM pg_control_checkpoint();'),
	't', 'resolved dependencies permit a newer restartpoint');

$primary->stop;
done_testing();
