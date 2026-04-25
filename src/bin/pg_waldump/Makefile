# src/bin/pg_waldump/Makefile

PGFILEDESC = "pg_waldump - decode and display WAL"
PGAPPICON=win32

# make these available to TAP test scripts
export TAR

subdir = src/bin/pg_waldump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	archive_waldump.o \
	compat.o \
	pg_waldump.o \
	rmgrdesc.o \
	xlogreader.o \
	xlogstats.o

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils

RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc*.c)))

# Umbra adds rmgrdesc/umbradesc.c, which should only be built when Umbra is
# enabled.  pg_waldump uses a wildcard to compile all rmgrdesc sources, so we
# must explicitly filter it out for md builds.
ifneq ($(with_umbra), yes)
RMGRDESCSOURCES := $(filter-out umbradesc.c,$(RMGRDESCSOURCES))
endif

RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))


all: pg_waldump

pg_waldump: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

xlogstats.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_waldump$(X) '$(DESTDIR)$(bindir)/pg_waldump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_waldump$(X)'

clean distclean:
	rm -f pg_waldump$(X) $(OBJS) $(RMGRDESCSOURCES) xlogreader.c xlogstats.c
	rm -f umbradesc.c umbradesc.o umbradesc.bc
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
