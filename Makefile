# $Id$

ROOTDIR=..
include ${ROOTDIR}/Makefile.path

PROG=		psync
SRCS+=		io.c
SRCS+=		options.c
SRCS+=		psync.c
SRCS+=		rpc.c
SRCS+=		stream.c
SRCS+=		util.c
MODULES+=	pfl gcrypt curses

include ${MAINMK}
