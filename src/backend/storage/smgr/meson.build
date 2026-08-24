# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'bulk_write.c',
  'md.c',
  'smgr.c',
)

if get_option('umbra')
  backend_sources += files(
    'umbra.c',
    'umfile.c',
    'ummap.c',
  )
endif
