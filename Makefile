# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		psync
SRCS+=		psync.c
SRCS+=		${PFL_BASE}/alloc.c
SRCS+=		${PFL_BASE}/crc.c
SRCS+=		${PFL_BASE}/init.c
SRCS+=		${PFL_BASE}/log.c
MODULES+=	pfl str
DEFINES+=	-DVERSION=$$(svn info | awk '{ if ($$0 ~ /^Revision: /) print $$2 }')

include ${MAINMK}
