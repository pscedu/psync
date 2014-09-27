# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		psync
SRCS+=		psync.c
SRCS+=		puppet.c
SRCS+=		util.c
SRCS+=		stream.c
MODULES+=	pfl

include ${MAINMK}
