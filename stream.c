/* $Id$ */
/* %PSC_COPYRIGHT% */

/*
 * The streams API communicates the psync protocol over sockets.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/iostats.h"
#include "pfl/random.h"

#include "psync.h"
#include "rpc.h"

psc_atomic32_t psync_xid;

ssize_t
atomicio(int op, int fd, void *buf, size_t len)
{
	size_t rem = len;
	char *p = buf;
	int nerr = 0;
	ssize_t rc;

	for (; rem > 0; rem -= rc, p += rc) {
		if (op == IOP_READ)
			rc = read(fd, p, rem);
		else
			rc = write(fd, p, rem);
		if (rc == 0)
			break;
		if (rc == -1) {
			if (errno != EINTR)
				psync_fatal("%s sz=%zd rc=%zd",
				    op == IOP_READ ? "read" : "write",
				    rem, rc);
#define MAX_RETRY	10
			if (++nerr > MAX_RETRY)
				psync_fatalx("exceeded number of "
				    "retries");
			rc = 0;
		}
		psc_iostats_intv_add(&iostats, rc);
	}
	return (rc);
}

void
stream_init(struct stream *st, int rfd, int wfd)
{
	memset(st, 0, sizeof(*st));
	st->rfd = rfd;
	st->wfd = wfd;
	psc_mutex_init(&st->mut);
}

void
stream_sendxv(struct stream *st, uint64_t xid, int opc,
    struct iovec *iov, int nio)
{
	struct hdr hdr;
	int i;

	hdr.opc = opc;
	hdr.msglen = 0;
	for (i = 0; i < nio; i++)
		hdr.msglen += iov[i].iov_len;
	if (xid)
		hdr.xid = xid;
	else
		hdr.xid = psc_atomic32_inc_getnew(&psync_xid);

	atomicio_write(st->wfd, &hdr, sizeof(hdr));
	for (i = 0; i < nio; i++)
		atomicio_write(st->wfd, iov[i].iov_base,
		    iov[i].iov_len);
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

void
stream_release(struct stream *st)
{
	psc_mutex_unlock(&st->mut);
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
		cmdv = str_split(cmd);

		close(rfd[0]);
		close(wfd[1]);
		if (dup2(wfd[0], 0) == -1)
			err(1, "dup2");
		if (dup2(rfd[1], 1) == -1)
			err(1, "dup2");
		execvp(cmdv[0], cmdv);
		err(1, "exec %s", cmd);
	default:
		close(rfd[1]);
		close(wfd[0]);
		return (stream_create(rfd[0], wfd[1]));
	}
}

struct stream *
stream_get(void)
{
	struct stream *st;
	int i, rnd;

	rnd = psc_random32u(opt_streams);

	for (;;) {
		// XXX should do a real shuffle
		DYNARRAY_FOREACH(st, i, &streams) {
			st = psc_dynarray_getpos(&streams, (i + rnd) %
			    psc_dynarray_len(&streams));
			if (psc_mutex_trylock(&st->mut))
				return (st);
		}
	}
}

struct stream *
stream_create(int rfd, int wfd)
{
	struct stream *st;

	st = PSCALLOC(sizeof(*st));
	st->rfd = rfd;
	st->wfd = wfd;
	psc_mutex_init(&st->mut);
	return (st);
}
