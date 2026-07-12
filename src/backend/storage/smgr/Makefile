#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/smgr
#
# IDENTIFICATION
#    src/backend/storage/smgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/smgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	bulk_write.o \
	md.o \
	smgr.o

ifeq ($(with_umbra), yes)
OBJS += \
	umbra.o \
	umfile.o
endif

# Reconfiguration can change OBJS, the default smgr implementation, and
# USE_UMBRA-dependent sync request registration.
objfiles.txt smgr.o md.o: $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/common.mk
