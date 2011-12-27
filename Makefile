# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		psync
SRCS+=		psync.c
SRCS+=		${PFL_BASE}/psc_util/alloc.c
SRCS+=		${PFL_BASE}/psc_util/crc.c
SRCS+=		${PFL_BASE}/psc_util/init.c
SRCS+=		${PFL_BASE}/psc_util/log.c
MODULES+=	pfl str
DEFINES+=	-DVERSION=$$(svn info | awk '{ if ($$0 ~ /^Revision: /) print $$2 }')

include ${MAINMK}
