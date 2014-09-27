/* $Id$ */
/* %PSC_COPYRIGHT% */

/*
 * The streams API communicates the psync protocol over sockets.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

psc_atomic32_t psync_xid;

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

void
stream_init(struct stream *st, int rfd, int wfd)
{
	memset(st, 0, sizeof(*st));
	st->rfd = rfd;
	st->wfd = wfd;
	psc_mutex_init(&st->mut);
}

void
stream_send(struct stream *st, int opc, void *p, size_t len)
{
	hdr.opc = opc;
	hdr.msglen = len;
	hdr.xid = psc_atomic32_inc_getnew(&psync_xid) - 1;

	atomic_write(st->wfd, &hdr, sizeof(hdr));
	atomic_write(st->wfd, p, len);
}

void
stream_release(struct stream *st)
{
	psc_mutex_unlock(&st->mut);
}

int
stream_cmdopen(struct stream *st, const char *fmt, ...)
{
	int rc, rfds[2], wfds[2];
	char *cmd, **cmdv;
	va_list ap;

	rc = socketpair(AF_LOCAL, SOCK_STREAM, PF_UNSPEC, rfds);
	if (rc == -1)
		err(1, "socketpair");

	rc = socketpair(AF_LOCAL, SOCK_STREAM, PF_UNSPEC, wfds);
	if (rc == -1)
		err(1, "socketpair");

	rc = fork();
	switch (rc) {
	case -1:
		err(1, "fork");
		break;
	case 0:
		va_start(ap, fmt);
		vasprintf(&cmd, fmt, ap);
		va_end(ap);

		cmdv = str_split(cmd);

		if (dup2(rfds[1], 0) == -1)
			err(1, "dup2");
		if (dup2(wfds[1], 1) == -1)
			err(1, "dup2");
		execvp(cmdv[0], cmdv);
		err(1, "exec %s", cmd);
		break;
	default:
		st->rfd = rfds[0];
		st->wfd = wfds[0];
		break;
	}
}

