# Copyright (c) 2022-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('pg_upgrade');
program_version_ok('pg_upgrade');
program_options_handling_ok('pg_upgrade');

if (check_pg_config('^#define USE_UMBRA 1$'))
{
	command_fails_like(
		['pg_upgrade'],
		qr/pg_upgrade is not supported with Umbra storage/,
		'pg_upgrade is rejected with Umbra storage');
}

done_testing();
