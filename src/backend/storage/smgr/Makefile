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

# Reconfiguration can change both OBJS and the default smgr implementation.
objfiles.txt smgr.o: $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/common.mk
