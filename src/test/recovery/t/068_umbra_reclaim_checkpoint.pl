# Verify that reclaim never removes the partial segment containing the
# append-only physical frontier.
use strict;
use warnings;

use Fcntl qw(O_CREAT O_EXCL O_WRONLY);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_reclaim_checkpoint');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
checkpoint_timeout = '30min'
max_wal_size = '4GB'
log_min_messages = debug1
map_compactor_enable = off
map_compactor_max_moves = 1
mapcompactor_delay = '10ms'
mapcompactor_max_relations = 10000
map_prealloc_main_low = 1073741823
});
$node->start;

$node->safe_psql(
	'postgres', q{
CREATE TABLE reclaim_vm_t(id int primary key, payload text);
INSERT INTO reclaim_vm_t
SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;
VACUUM (FREEZE, ANALYZE) reclaim_vm_t;
});

my $vm_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('reclaim_vm_t') || '_vm';});
my $vm_abspath = $node->data_dir . '/' . $vm_path;

ok(-e $vm_abspath, 'visibility map physical segment exists before truncate');
cmp_ok(
	$node->safe_psql(
		'postgres',
		q{SELECT pg_relation_size('reclaim_vm_t', 'vm');}),
	'>', 0, 'visibility map has a mapped physical page');

$node->safe_psql(
	'postgres', q{
DELETE FROM reclaim_vm_t;
VACUUM (TRUNCATE TRUE, DISABLE_PAGE_SKIPPING) reclaim_vm_t;
});

# Truncate removed every VM MAP reference, but the segment still contains the
# append-only physical frontier.  It cannot be unlinked safely: another backend
# may retain an FD for that inode and allocate a later P in the same segment.
ok(-e $vm_abspath,
	'partial visibility map segment remains before checkpoint');

$node->safe_psql('postgres', 'CHECKPOINT');
ok(-e $vm_abspath,
	'checkpoint does not reclaim the append-only frontier segment');

# A later first-born VM page reuses neither P nor the pathname through a stale
# descriptor; it advances within the retained frontier segment.
$node->safe_psql(
	'postgres', q{
INSERT INTO reclaim_vm_t
SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;
VACUUM (FREEZE, ANALYZE) reclaim_vm_t;
});
ok(-e $vm_abspath, 'visibility map frontier segment remains addressable');
is($node->safe_psql('postgres', 'SELECT count(*) FROM reclaim_vm_t;'),
	'2000', 'relation remains readable after auxiliary segment reclaim');

$node->restart;
is($node->safe_psql('postgres', 'SELECT count(*) FROM reclaim_vm_t;'),
	'2000', 'relation remains readable after clean restart');

# Start with an empty relation whose generation and MAP root are durable.
$node->safe_psql(
	'postgres', qq{
CREATE TABLE reclaim_restart_t(id integer, payload text)
  WITH (autovacuum_enabled = false);
ALTER TABLE reclaim_restart_t ALTER COLUMN payload SET STORAGE PLAIN;
CHECKPOINT;
});

my $restart_path = $node->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('reclaim_restart_t');});
my $restart_abspath = $node->data_dir . '/' . $restart_path;
my $restart_seg1 = "$restart_abspath.1";
my $block_size = 0 + $node->safe_psql(
	'postgres', q{SELECT pg_size_bytes(current_setting('block_size'));});

ok(-e $restart_abspath, 'empty MAIN segment exists before injection');
is(-s $restart_abspath, 0, 'empty MAIN segment has no physical blocks');
ok(!-e $restart_seg1, 'no MAIN tail segment exists before injection');

$node->stop('immediate');

# Inject one old physical block into seg0 and a sparse high-water block into
# seg1.  Neither has a MAP reference.  Root loading reconciles the resident
# frontier to RELSEG_SIZE + 1, making seg0 reclaimable while seg1 remains the
# append-only high-water sentinel.
sysopen(my $old_fh, $restart_abspath, O_WRONLY)
  or BAIL_OUT("could not open old segment \"$restart_abspath\": $!");
binmode($old_fh);
my $old_written = syswrite($old_fh, "\0" x $block_size);
defined($old_written) && $old_written == $block_size
  or BAIL_OUT("could not write old segment \"$restart_abspath\": $!");
close($old_fh)
  or BAIL_OUT("could not close old segment \"$restart_abspath\": $!");

sysopen(my $tail_fh, $restart_seg1, O_WRONLY | O_CREAT | O_EXCL, 0600)
  or BAIL_OUT("could not create orphan sparse tail \"$restart_seg1\": $!");
binmode($tail_fh);
my $tail_written = syswrite($tail_fh, "\0" x $block_size);
defined($tail_written) && $tail_written == $block_size
  or BAIL_OUT("could not write orphan sparse tail \"$restart_seg1\": $!");
close($tail_fh)
  or BAIL_OUT("could not close orphan sparse tail \"$restart_seg1\": $!");

$node->start;
is(-s $restart_abspath, $block_size,
	'crash restart retains the unreferenced old segment');
is(-s $restart_seg1, $block_size,
	'root load retains the one-block orphan high-water segment');

# Scanning the relation makes the durable root resident.  The compactor can now
# scan the empty MAP, discover seg0, and put it through normal checkpoint aging.
# Repeated checkpoints only remove scheduling races from the test.
is($node->safe_psql(
		'postgres', q{SELECT count(*) FROM reclaim_restart_t;}),
	'0', 'empty logical EOF is readable before reclaim discovery');
$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
});

for (1 .. 100)
{
	$node->safe_psql('postgres', 'CHECKPOINT;');
	last if !-e $restart_abspath;
	usleep(100_000);
}
ok(!-e $restart_abspath,
	'periodic discovery reclaims a complete unreferenced segment after restart');
ok(-e $restart_seg1,
	'periodic discovery preserves the append-only high-water segment');
is($node->safe_psql('postgres', 'SELECT count(*) FROM reclaim_restart_t;'),
	'0', 'relation remains readable after reconstructed reclaim');

$node->restart;
ok(!-e $restart_abspath,
	'reclaimed old segment stays absent after clean restart');
ok(-e $restart_seg1,
	'high-water segment remains after clean restart');
is($node->safe_psql('postgres', 'SELECT count(*) FROM reclaim_restart_t;'),
	'0', 'empty relation remains readable after clean restart');

$node->safe_psql(
	'postgres', q{
INSERT INTO reclaim_restart_t VALUES (1, repeat('n', 7800));
CHECKPOINT;
});
ok(!-e $restart_abspath,
	'new allocation after reclaim does not recreate the old segment hole');
ok(-e $restart_seg1, 'new allocation remains in the high-water segment');

$node->restart;
is($node->safe_psql('postgres', 'SELECT count(*) FROM reclaim_restart_t;'),
	'1', 'relation remains writable across restart after reconstructed reclaim');

if (($ENV{enable_injection_points} // '') eq 'yes' &&
	$node->check_extension('injection_points'))
{
	$node->safe_psql('postgres', q{
CREATE EXTENSION injection_points;
ALTER SYSTEM SET map_compactor_enable = off;
SELECT pg_reload_conf();
DROP TABLE reclaim_vm_t;
});
	$node->stop;

	# Recreate an unreferenced old segment below the mapped page in seg1.  The
	# paused UPDATE below holds the relation MAP extension lock between reserving
	# its new P and installing the shared pending range, so reclaim cannot observe
	# that otherwise unprotected window.
	sysopen(my $reserved_fh, $restart_abspath,
		O_WRONLY | O_CREAT | O_EXCL, 0600)
	  or BAIL_OUT("could not recreate old segment \"$restart_abspath\": $!");
	binmode($reserved_fh);
	my $reserved_written = syswrite($reserved_fh, "\0" x $block_size);
	defined($reserved_written) && $reserved_written == $block_size
	  or BAIL_OUT("could not write old segment \"$restart_abspath\": $!");
	close($reserved_fh)
	  or BAIL_OUT("could not close old segment \"$restart_abspath\": $!");

	$node->start;
	is($node->safe_psql('postgres',
			q{SELECT count(*) FROM reclaim_restart_t;}),
		'1', 'relation root is resident before reclaim discovery');
	$node->safe_psql('postgres', q{
SELECT injection_points_attach(
  'umbra-reclaim-after-discovery', 'wait');
SELECT injection_points_attach(
  'umbra-reclaim-before-extension-lock', 'wait');
SELECT injection_points_attach(
  'umbra-reclaim-extension-lock-busy', 'wait');
SELECT injection_points_attach(
  'umbra-reclaim-before-unlink', 'wait');
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
});

	# First let the compactor discover seg0, then checkpoint-age that exact task.
	$node->wait_for_event('map compactor', 'umbra-reclaim-after-discovery');
	pass('compactor discovers the synthetic unreferenced segment');
	$node->safe_psql('postgres', q{
SELECT injection_points_detach('umbra-reclaim-after-discovery');
SELECT injection_points_wakeup('umbra-reclaim-after-discovery');
CHECKPOINT;
});
	# The worker now pauses immediately before trying the extension lock.
	$node->wait_for_event(
		'map compactor', 'umbra-reclaim-before-extension-lock');
	pass('mature reclaim task pauses before taking the extension lock');

	$node->safe_psql('postgres', q{
SELECT injection_points_attach(
  'umbra-remap-after-reserve-before-pending', 'wait');
});
	my $writer = $node->background_psql('postgres');
	# The extension-lock hook runs inside WAL insertion's critical section.
	$writer->query_safe(q{SELECT injection_points_load(
  'umbra-remap-after-reserve-before-pending')});
	$writer->query_safe(
		q{SELECT payload FROM reclaim_restart_t WHERE id = 1});
	$writer->query_until(
		qr/starting_reserved_remap/,
		q(
\echo starting_reserved_remap
UPDATE reclaim_restart_t SET payload = repeat('r', 7800) WHERE id = 1;
\echo reserved_remap_done
\q
));
	$node->wait_for_event(
		'client backend', 'umbra-remap-after-reserve-before-pending');
	pass('remap pauses after reserving P and before installing its pending range');

	# Let reclaim attempt its conditional lock while remap owns the lock.  The
	# second hook proves that it observed the conflict instead of unlinking seg0.
	$node->safe_psql('postgres', q{
SELECT injection_points_wakeup('umbra-reclaim-before-extension-lock');
});
	$node->wait_for_event(
		'map compactor', 'umbra-reclaim-extension-lock-busy');
	ok(-e $restart_abspath,
		'extension lock protects reserve-to-pending reclaim window');

	$node->safe_psql('postgres', q{
SELECT injection_points_detach('umbra-reclaim-before-extension-lock');
SELECT injection_points_detach('umbra-reclaim-extension-lock-busy');
SELECT injection_points_wakeup('umbra-reclaim-extension-lock-busy');
SELECT injection_points_wakeup(
  'umbra-remap-after-reserve-before-pending');
});
	$writer->{run}->finish;
	$node->safe_psql('postgres', q{
SELECT injection_points_detach(
  'umbra-remap-after-reserve-before-pending');
});
	is($node->safe_psql(
			'postgres', q{SELECT payload = repeat('r', 7800)
						  FROM reclaim_restart_t WHERE id = 1;}),
		't', 'reserved remap completes after publication resumes');

	$node->safe_psql('postgres', 'CHECKPOINT;');
	$node->wait_for_event('map compactor', 'umbra-reclaim-before-unlink');
	pass('reclaim rechecks reservations and MAP before unlink');
	$node->safe_psql('postgres', q{
SELECT injection_points_detach('umbra-reclaim-before-unlink');
SELECT injection_points_wakeup('umbra-reclaim-before-unlink');
});
	for (1 .. 100)
	{
		last if !-e $restart_abspath;
		usleep(10_000);
	}
	ok(!-e $restart_abspath,
		'worker reclaims the old segment after the extension lock is released');
}

$node->stop;
done_testing();
