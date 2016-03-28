/* $Id$ */
/*
 * %ISC_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright (c) 2011-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the
 * above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN NO EVENT SHALL THE
 * AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
 * DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR
 * PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
 * TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 * --------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * The streams API communicates the psync protocol over sockets.
 */

#include <sys/types.h>
#include <sys/uio.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/opstats.h"
#include "pfl/random.h"
#include "pfl/str.h"

#include "psync.h"
#include "rpc.h"

psc_atomic64_t psync_xid;

ssize_t
atomicio(int op, int fd, void *buf, size_t len)
{
	size_t rem = len;
	char *p = buf;
	ssize_t rc = 0;
	int nerr = 0;

	for (; rem > 0; rem -= rc, p += rc) {
		if (op == IOP_READ)
			rc = read(fd, p, rem);
		else
			rc = write(fd, p, rem);
		if (rc == 0)
			break;
		if (rc == -1) {
			if (errno != EINTR)
				psync_fatal("%s sz=%zd",
				    op == IOP_READ ? "read" : "write",
				    rem);
#define MAX_RETRY	10
			if (++nerr > MAX_RETRY)
				psync_fatalx("exceeded number of "
				    "retries");
			rc = 0;
		}
		if (iostats)
			pfl_opstat_add(iostats, rc);
	}
	return (rc);
}

void
stream_sendxv(struct stream *st, uint64_t xid, int opc,
    struct iovec *iov, int nio)
{
	struct hdr hdr;
	int i;

	hdr.magic = PSYNC_MAGIC;
	hdr.opc = opc;
	hdr.msglen = 0;
	for (i = 0; i < nio; i++)
		hdr.msglen += iov[i].iov_len;
	if (xid)
		hdr.xid = xid;
	else
		hdr.xid = psc_atomic64_inc_getnew(&psync_xid);

	spinlock(&st->lock);
	atomicio_write(st->wfd, &hdr, sizeof(hdr));
	for (i = 0; i < nio; i++)
		atomicio_write(st->wfd, iov[i].iov_base,
		    iov[i].iov_len);
	freelock(&st->lock);
}

void
stream_sendx(struct stream *st, uint64_t xid, int opc, void *p,
    size_t len)
{
	struct iovec iov;

	iov.iov_base = p;
	iov.iov_len = len;
	stream_sendxv(st, xid, opc, &iov, 1);
}

struct stream *
stream_cmdopen(const char *fmt, ...)
{
	int rfd[2], wfd[2];
	char *cmd, **cmdv;
	va_list ap;

	if (pipe(rfd) == -1)
		err(1, "pipe");
	if (pipe(wfd) == -1)
		err(1, "pipe");

	switch (fork()) {
	case -1:
		err(1, "fork");
	case 0:
		va_start(ap, fmt);
		vasprintf(&cmd, fmt, ap);
		va_end(ap);
		cmdv = pfl_str_split(cmd);

		close(rfd[0]);
		close(wfd[1]);
		if (dup2(wfd[0], 0) == -1)
			err(1, "dup2");
		if (dup2(rfd[1], 1) == -1)
			err(1, "dup2");

		setsid();

		execvp(cmdv[0], cmdv);
		err(1, "exec %s", cmd);
	default:
		close(rfd[1]);
		close(wfd[0]);
		return (stream_create(rfd[0], wfd[1]));
	}
}

struct stream *
stream_create(int rfd, int wfd)
{
	struct stream *st;

	st = PSCALLOC(sizeof(*st));
	INIT_SPINLOCK(&st->lock);
	st->rfd = rfd;
	st->wfd = wfd;
	push(&streams, st);
	return (st);
}
