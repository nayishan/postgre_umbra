# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that logical pages sharing an active slot can use one physical read.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

for my $io_method (qw(sync worker))
{
	my $node = PostgreSQL::Test::Cluster->new(
		'umbra_mapped_readv_' . $io_method);

	$node->init(no_data_checksums => 1);
	$node->append_conf(
		'postgresql.conf', qq[
autovacuum = off
bgwriter_lru_maxpages = 0
checkpoint_timeout = '1h'
full_page_writes = on
io_method = '$io_method'
]);
	$node->start;

	$node->safe_psql('postgres', q[
CREATE EXTENSION test_aio;
CREATE TABLE umbra_mapped_readv(id integer, payload text)
  WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO umbra_mapped_readv
SELECT g, repeat('x', 100) FROM generate_series(1, 1000) AS g;
CHECKPOINT;
SELECT evict_rel('umbra_mapped_readv');
]);

	is($node->safe_psql(
			'postgres', q[
SELECT blockoff, blocknum, io_reqd, nblocks
FROM read_buffers('umbra_mapped_readv', 0, 2);]),
		'0|0|t|2',
		"$io_method combines two contiguous mapped pages");

	$node->safe_psql('postgres', q[
CHECKPOINT;
UPDATE umbra_mapped_readv SET payload = 'changed' WHERE id = 1;
SELECT evict_rel('umbra_mapped_readv');
]);

	is($node->safe_psql(
			'postgres', q[
SELECT blockoff, blocknum, io_reqd, nblocks
FROM read_buffers('umbra_mapped_readv', 0, 2);]),
		"0|0|t|1\n1|1|t|1",
		"$io_method splits pages on an active-slot boundary");

	$node->stop;
}

done_testing();
