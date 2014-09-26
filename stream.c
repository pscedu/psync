/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void
stream_init(struct stream *st, int rfd, int wfd)
{
	memset(st, 0, sizeof(*st));
	st->rfd = rfd;
	st->wfd = wfd;
	psc_mutex_init(&st->mut);
}

ssize_t
atomicio(int op, int fd, void *buf, size_t len)
{
	size_t off = 0, rem = len;
	char *p = buf;
	int nerr = 0;
	ssize_t rc;

	for (; rem > 0; rem -= rc, p += rc) {
		if (op == IOP_READ)
			rc = read(fd, p, rem);
		else
			rc = write(fd, p, rem);
		if (rc == -1) {
			if (errno != EINTR)
				err(1, "%s", op == IOP_READ ?
				    "read" : "write");
			if (++nerr > MAX_RETRY)
				errx(1, "exceeded number of retries");
			rc = 0;
		}
	}
	return (rc);
}

psc_atomic32_t psync_xid;

void
stream_send(struct stream *st, int opc, void *p, size_t len)
{
	hdr.opc = opc;
	hdr.msglen = len;
	hdr.xid = psc_atomic32_inc_getold(&psync_xid);

	atomic_write(st->wfd, &hdr, sizeof(hdr));
	atomic_write(st->wfd, p, len);
}

void
stream_release(struct stream *st)
{
	psc_mutex_unlock(&st->mut);
}