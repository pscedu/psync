# $Id$

ROOTDIR=..
include ${ROOTDIR}/Makefile.path

PROG=		psync
MAN+=		psync.1
SRCS+=		io.c
SRCS+=		options.c
SRCS+=		psync.c
SRCS+=		rpc.c
SRCS+=		stream.c
SRCS+=		util.c
MODULES+=	pfl gcrypt curses
DEFINES+=	-DPSYNC_VERSION=$$(git log | grep -c ^commit)

include ${MAINMK}
