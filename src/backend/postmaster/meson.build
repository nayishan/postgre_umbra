# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'autovacuum.c',
  'auxprocess.c',
  'bgworker.c',
  'bgwriter.c',
  'checkpointer.c',
  'datachecksum_state.c',
  'fork_process.c',
  'interrupt.c',
  'launch_backend.c',
  'pgarch.c',
  'pmchild.c',
  'postmaster.c',
  'startup.c',
  'syslogger.c',
  'walsummarizer.c',
  'walwriter.c',
)

if get_option('umbra').enabled()
  backend_sources += files(
    'mapwriter.c',
  )
endif
