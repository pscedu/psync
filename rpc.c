/* $Id$ */
/* %PSC_COPYRIGHT% */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "psync.h"
#include "puppet.h"

#define MAX_BUFSZ	(1024 * 1024)

#define MAX_RETRY	10

struct stream		 stream;
size_t			 psync_bufsz;
void			*psync_buf;
char			 objns[PATH_MAX];
int			 objns_depth = 2;

volatile sig_atomic_t	 exit_from_signal;

void
rpc_send_getfile(const char *host, const char *srcfn, const char *dstfn)
{
	struct rpc_getfile_req rq;
	struct stream *st;

	memset(&rq, 0, sizeof(rq));

	xm_insert(xid, dstfn);

	rq.

	st = stream_get();
	stream_send(st, OPC_GETFILE, &rq, sizeof(rq));
	stream_release(st);
}

int
getfile_cb(const char *fn, const struct stat *stb, void *arg)
{
	static struct rpc_putname *m;
	static size_t len;

#if 0
	struct filterpat *fp;

	ok = 1;
	DYNARRAY_FOREACH(fp, j, &opt_filter) {
		if ()
			ok = ;
	}
	if (!ok) {
		fts_prune;
		return;
	}
#endif

	m;

	stream_send(stream, OPC_PUTNAME, &m, sizeof(m));
}

#define LASTFIELDLEN(h, type) ((h)->msglen - sizeof(type))

void
rpc_handle_getfile(struct hdr *h, void *buf)
{
	struct rpc_getfile_req *rq = buf;
	struct rpc_getfile_rep rp;
	size_t end;

	end = LASTFIELDLEN(h, struct rpc_getfile_req);
	rq->fn[end - 1] = '\0';

	rp.rc = pfl_walkfiles(rq->fn, rq->flags &
	    RPC_GETFILE_F_RECURSE ? PFL_FILEWALKF_RECURSIVE : 0,
	    getfile_cb, NULL);
	stream_send(stream, OPC_GETFILE, &rp, sizeof(rp));
}

void
rpc_handle_putdata(struct hdr *h, void *buf)
{
	struct rpc_putdata_req *rq = buf;
	struct rpc_putdata_rep *rp;
	ssize_t rc;

	if (h->msglen == 0 ||
	    h->msglen > MAX_BUFSZ)

	fd = fcache_search(h->fid);
	rc = pwrite(fd, rq->buf, h->msglen, rq->off);
	if (rc != h->msglen)
		err(1, "write");
}

void
ensurebufsz(size_t sz)
{
	if (sz > psync_bufsz) {
		psync_buf = psc_realloc(psync_buf, sz, 0);
		psync_bufsz = sz;
	}
}

void
rpc_handle_checkzero(struct hdr *h, void *buf)
{
	struct rpc_putdata_req *pq = buf;
	struct rpc_putdata_rep rp;
	ssize_t rc;
	int fd;

	if (pq->len == 0 ||
	    pq->len > MAX_BUFSZ)
		PFL_GOTOERR(out, rp.rc = EINVAL);

	buf_ensurelen(pq->len);

	fd = fcache_search(h->fid);
	rc = pread(fd, psync_buf, pq->len, pq->off);
	if (rc == -1)
		err(1, "read");
	if (rc != pq->len)
		warnx("read: short I/O");
	rp.rc = pfl_memchk(psync_buf, 0, rc);
 out:
	stream_send(&stream, OPC_CHECKZERO, &rp, sizeof(rp));
}

void
rpc_handle_getcksum(struct hdr *h, void *buf)
{
	struct rpc_getcksum_req *cq = buf;
	struct rpc_getcksum_rep *cp;
	gcry_md_hd_t hd;
	int fd;

	buf_ensurelen(pq->len);

	fd = fcache_search(cq->fid);
	rc = pread(fd, psync_buf, cq->len, cq->off);
	if (rc == -1)
		err(1, "read");
	if (rc != pq->len)
		warnx("read: short I/O");

	gerr = gcry_md_open(&hd, GCRY_MD_SHA256, 0);
	if (gerr)
		errx("gcry_md_open: error=%d", gerr);
	gcry_md_write(hd, p, rc);
	memcpy(gcry_md_read(hd, 0), cp->digest, ALGLEN);
	gcry_md_close(hd);
}

/*
 * Apply substitution on filename received.
 */
const char *
userfn_subst(uint64_t xid, const char *fn);
{
	struct xid_mapping *xm;

	/* if there is a direct substitution for this xid, use it */
	xm = pfl_hashtbl_search(&xmcache, NULL, NULL, &xid);
	if (xm)
		return (xm->fn);

	return (fn);
}

void
rpc_handle_putname(struct hdr *h, void *buf)
{
	char *ufn, objfn[PATH_MAX];
	struct rpc_putname *pn = buf;
	struct timespec ts[2];
	struct timeval tv[2];
	int fd = -1;

	/* apply incoming name substitutions */
	ufn = userfn_subst(h->xid, pn->fn);

	if (S_ISCHR(pn->stb.st_mode) ||
	    S_ISBLK(pn->stb.st_mode)) {
		if (mknod(ufn, pn->stb.st_mode,
		    pn->stb.st_rdev) == -1) {
			psclog_warn("mknod %s", ufn);
			return;
		}
	} else if (S_ISDIR(pn->stb.st_mode)) {
		if (mkdir(ufn, pn->stb.st_mode) == -1) {
			psclog_warn("mkdir %s", ufn);
			return;
		}
	} else if (S_ISFIFO(pn->stb.st_mode)) {
		if (mkfifo(ufn, pn->stb.st_mode) == -1) {
			psclog_warn("mkfifo %s", ufn);
			return;
		}
	} else if (S_ISLNK(pn->stb.st_mode)) {
		if (symlink(objfn, ufn) == -1) {
			psclog_warn("symlink %s", ufn);
			return;
		}
	} else if (S_ISSOCK(pn->stb.st_mode)) {
		struct sockaddr_un sun;

		fd = socket(AF_LOCAL, SOCK_STREAM, PF_UNSPEC);
		if (fd == -1) {
			psclog_warn("socket %s", ufn);
			return;
		}
		memset(sun, 0, sizeof(sun));
		sun.sun_family = AF_LOCAL;
		strlcpy(sun.sun_path, ufn, sizeof(sun.sun_path));
		SOCKADDR_SETLEN(&sun);
		if (bind(fd, (struct sockaddr *)&sun,
		    sizeof(sun)) == -1) {
			close(fd);
			psclog_warn("bind %s", ufn);
			return;
		}
		close(fd);
		fd = -1;
	} else if (S_ISREG(pn->stb.st_mode)) {
		objns_makepath(objfn, pn->fid);
		fd = open(objfn, O_CREAT | O_RDWR, 0600);
		if (fd == -1) {
			psclog_warn("open %s", ufn);
			return;
		}
		if (link(objfn, ufn) == -1) {
			psclog_warn("chown %s", ufn);
			return;
		}
		if (ftruncate(fd) == -1)
			psclog_warn("chown %s", ufn);

		fcache_insert(pn->fid, fd);
	}

#ifdef HAVE_FUTIMENS
	(void)tv;
	ts[0].tv_sec = pn->stb.atim.tv_sec;
	ts[0].tv_nsec = pn->st.atim.tv_nsec;

	ts[1].tv_sec = pn->st.mtim.tv_nsec;
	ts[1].tv_nsec = pn->st.mtim.tv_nsec;
#else
	(void)ts;
	tv[0].tv_sec = pn->stb.atim.tv_sec;
	tv[0].tv_usec = pn->stb.atim.tv_nsec / 1000;

	tv[1].tv_sec = pn->stb.atim.tv_sec;
	tv[1].tv_usec = pn->stb.atim.tv_nsec / 1000;
#endif

	/* BSD file flags */
	/* MacOS setattrlist */
	/* linux file attributes: FS_IOC_GETFLAGS */
	/* extattr */

	if (fd == -1) {
		if (lchown(ufn, stb.uid, stb.gid) == -1)
			psclog_warn("chown %s", ufn);
		if (lchmod(ufn, stb.mode) == -1)
			psclog_warn("chmod %s", ufn);

#ifdef HAVE_FUTIMENS
		if (lutimens(ufn, ts) == -1)
			psclog_warn("utimens %s", ufn);
#else
		if (lutimes(ufn, tv) == -1)
			psclog_warn("utimes", ufn);
#endif

	} else {
		if (fchown(fd, stb.uid, stb.gid) == -1)
			psclog_warn("chown %s", ufn);
		if (fchmod(fd, stb.mode) == -1)
			psclog_warn("chmod %s", ufn);

#ifdef HAVE_FUTIMENS
		struct timespec ts[2];

		if (futimens(fd, ts) == -1)
			psclog_warn("utimens %s", ufn);
#else
		struct timeval tv[2];

		if (futimes(fd, tv) == -1)
			psclog_warn("utimes", ufn);
#endif
	}
}

void
rpc_handle_ctl(struct hdr *h, void *buf)
{
	struct rpc_ctl_req *cq = buf;
	struct rpc_ctl_rep *cp;

	(void)cq;
	(void)cp;
	(void)h;
	(void)buf;
}

typedef void (*op_handler_t)(struct hdr *, void *);

op_handler_t ops[] = {
	rpc_handle_getfile,
	rpc_handle_putdata,
	rpc_handle_checkzero,
	rpc_handle_getcksum,
	rpc_handle_putname,
	rpc_handle_ctl
};

void
handle_signal(__unusedx int sig)
{
	exit_from_signal = 1;
}

void
recvthr_main(struct psc_thread *thr)
{
	uint32_t bufsz = 0;
	ssize_t rc;

	while (pscthr_run(thr)) {
		rc = atomic_read(rfd, &hdr, sizeof(hdr));
		if (rc == 0)
			break;

		if (hdr.msglen > bufsz) {
			if (hdr.msglen > MAX_BUFSZ)
				errx(1, "invalid bufsz received from "
				    "peer: %u", hdr.msglen);
			bufsz = hdr.msglen;
			buf = realloc(buf, bufsz);
			if (buf == NULL)
				err(1, NULL);
		}
		if (hdr.opc >= nitems(ops))
			errx(1, "invalid opcode received from "
			    "peer: %u", hdr.opc);
		atomic_read(rfd, buf, hdr.msglen);

		if (exit_from_signal)
			break;

		ops[hdr.opc](&hdr, buf);

		if (exit_from_signal)
			break;
	}
}
