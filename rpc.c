/* $Id$ */
/* %PSC_COPYRIGHT% */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <fcntl.h>
#include <gcrypt.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/alloc.h"
#include "pfl/net.h"
#include "pfl/pool.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/walk.h"

#include "psync.h"
#include "rpc.h"

#define MAX_BUFSZ	(1024 * 1024)

char			 objns_path[PATH_MAX];
int			 objns_depth = 2;

volatile sig_atomic_t	 exit_from_signal;

void *
buf_get(size_t len)
{
	struct buf *b;

	b = psc_pool_get(buf_pool);
	if (len > b->len) {
		b->buf = psc_realloc(b->buf, len, 0);
		// memset(b->buf + oldlen, 0, len - oldlen);
		b->len = len;
	}
	return (b);

}

#define buf_release(b)	psc_pool_return(buf_pool, (b))

void
rpc_send_getfile(uint64_t xid, const char *fn, const char *base)
{
	struct rpc_getfile_req gfq;
	struct iovec iov[3];
	struct stream *st;

	memset(&gfq, 0, sizeof(gfq));

	iov[0].iov_base = &gfq;
	iov[0].iov_len = sizeof(gfq);

	iov[1].iov_base = (void *)fn;
	iov[1].iov_len = gfq.len = strlen(fn) + 1;
psynclog_tdebug("SEND GETFILE %lx", xid);

	iov[2].iov_base = (void *)base;
	iov[2].iov_len = strlen(fn) + 1;

	st = stream_get();
	stream_sendxv(st, xid, OPC_GETFILE_REQ, iov, nitems(iov));
	stream_release(st);
}

void
rpc_send_putdata(uint64_t fid, off_t off, const void *buf, size_t len)
{
	struct rpc_putdata pd;
	struct iovec iov[2];
	struct stream *st;

	memset(&pd, 0, sizeof(pd));
	pd.fid = fid;
	pd.off = off;
//psynclog_tdebug("PUT %lx", pd.fid);

	iov[0].iov_base = &pd;
	iov[0].iov_len = sizeof(pd);

	iov[1].iov_base = (void *)buf;
	iov[1].iov_len = len;

	st = stream_get();
	stream_sendv(st, OPC_PUTDATA, iov, nitems(iov));
	stream_release(st);
}

void
rpc_send_putname(const char *dir, const char *fn,
    const struct stat *stb)
{
	struct rpc_putname pn;
	struct iovec iov[3];
	struct stream *st;

	memset(&pn, 0, sizeof(pn));
	pn.fid = stb->st_ino;
	pn.pstb.dev = stb->st_dev;
	pn.pstb.rdev = stb->st_rdev;
	pn.pstb.mode = stb->st_mode;
	pn.pstb.uid = stb->st_uid;
	pn.pstb.gid = stb->st_gid;
	pn.pstb.size = stb->st_size;
	PFL_STB_ATIME_GET(stb, &pn.pstb.atim.tv_sec,
	    &pn.pstb.atim.tv_nsec);
	PFL_STB_MTIME_GET(stb, &pn.pstb.mtim.tv_sec,
	    &pn.pstb.mtim.tv_nsec);

	iov[0].iov_base = &pn;
	iov[0].iov_len = sizeof(pn);

	iov[1].iov_base = (void *)dir;
	iov[1].iov_len = pn.dirlen = strlen(dir) + 1;	// up to PATH_MAX

	iov[2].iov_base = (void *)fn;
	iov[2].iov_len = strlen(fn) + 1;		// up to NAME_MAX

	st = stream_get();
	stream_sendv(st, OPC_PUTNAME, iov, nitems(iov));
	stream_release(st);
}

void
rpc_send_done(struct stream *st, int clean)
{
	struct rpc_done d;
	struct iovec iov;
psynclog_tdebug("send_done");

	d.clean = clean;

	iov.iov_base = &d;
	iov.iov_len = sizeof(d);

	stream_sendv(st, OPC_DONE, &iov, 1);
}

#define LASTFIELDLEN(h, type) ((h)->msglen - sizeof(type))

void
rpc_handle_getfile_req(struct hdr *h, void *buf)
{
	struct rpc_getfile_req *gfq = buf;
	struct rpc_getfile_rep gfp;
	struct stream *st;
	struct walkarg wa;
	struct stat stb;
	char *base;
	int flags;

	base = gfq->fn + gfq->len;

	if (stat(gfq->fn, &stb) == 0) {
		char *p;

		flags = PFL_FILEWALKF_RELPATH;

		p = strrchr(gfq->fn, '/');
		if (p)
			wa.trim = p - gfq->fn;
		else
			wa.trim = 0;
		wa.prefix = base[0] ? base : ".";
		if (S_ISDIR(stb.st_mode) && opt_recursive)
			flags |= PFL_FILEWALKF_RECURSIVE;

		gfp.rc = pfl_filewalk(gfq->fn, flags, NULL,
		    push_putfile_walkcb, &wa);
	} else {
		gfp.rc = errno;
psynclog_tdebug("getfile %d", errno);
	}

	st = stream_get();
	stream_send(st, OPC_GETFILE_REP, &gfp, sizeof(gfp));
	stream_release(st);
}

void
rpc_handle_getfile_rep(struct hdr *h, void *buf)
{
	struct rpc_getfile_rep *gfp = buf;
	size_t end;

	(void)h;
	(void)gfp;
	(void)end;
}

void
rpc_handle_putdata(struct hdr *h, void *buf)
{
	struct rpc_putdata *pd = buf;
	ssize_t rc;
	size_t len;
	int fd;

#if 0
	if (h->msglen == 0 ||
	    h->msglen > MAX_BUFSZ) {
		psynclog_warn("invalid msglen");
		return;
	}
#endif

	len = h->msglen - sizeof(*pd);

//psynclog_tdebug("HANDLE PUT %lx", pd->fid);
	fd = fcache_search(pd->fid);
	rc = pwrite(fd, pd->data, len, pd->off);
	if (rc != (ssize_t)len)
		psynclog_error("write off=%"PRId64" len=%"PRId64" "
		    "rc=%zd", pd->off, len, rc);
}

void
rpc_handle_checkzero_req(struct hdr *h, void *buf)
{
	struct rpc_checkzero_req *czq = buf;
	struct rpc_checkzero_rep czp;
	struct stream *st;
	struct buf *bp;
	ssize_t rc;
	int fd;

#if 0
	if (czq->len == 0 ||
	    czq->len > MAX_BUFSZ)
		PFL_GOTOERR(out, czp.rc = EINVAL);
#endif

	bp = buf_get(czq->len);

	fd = fcache_search(czq->fid);
	rc = pread(fd, bp->buf, czq->len, czq->off);
	if (rc == -1)
		err(1, "read");
	if ((uint64_t)rc != czq->len)
		warnx("read: short I/O");
	czp.rc = pfl_memchk(bp->buf, 0, rc);

	buf_release(bp);

	st = stream_get();
	stream_sendx(st, h->xid, OPC_CHECKZERO_REP, &czp, sizeof(czp));
	stream_release(st);
}

void
rpc_handle_checkzero_rep(struct hdr *h, void *buf)
{
	struct rpc_checkzero_rep *czp = buf;

	(void)h;
	(void)czp;
}

void
rpc_handle_getcksum_req(struct hdr *h, void *buf)
{
	struct buf *bp;
	struct rpc_getcksum_req *gcq = buf;
	struct rpc_getcksum_rep gcp;
	struct stream *st;
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	ssize_t rc;
	int fd;

#if 0
	if (czq->len == 0 ||
	    czq->len > MAX_BUFSZ)
		PFL_GOTOERR(out, czp.rc = EINVAL);
#endif

	bp = buf_get(gcq->len);

	fd = fcache_search(gcq->fid);
	rc = pread(fd, bp->buf, gcq->len, gcq->off);
	if (rc == -1)
		err(1, "read");
	if ((uint64_t)rc != gcq->len)
		warnx("read: short I/O");

	gerr = gcry_md_open(&hd, GCRY_MD_SHA256, 0);
	if (gerr)
		errx(1, "gcry_md_open: error=%d", gerr);
	gcry_md_write(hd, bp->buf, rc);
	memcpy(gcry_md_read(hd, 0), gcp.digest, ALGLEN);
	gcry_md_close(hd);

	buf_release(bp);

	st = stream_get();
	stream_sendx(st, h->xid, OPC_GETCKSUM_REP, &gcp, sizeof(gcp));
	stream_release(st);
}

void
rpc_handle_getcksum_rep(struct hdr *h, void *buf)
{
	struct rpc_getcksum_rep *gcp = buf;

	(void)h;
	(void)gcp;
}

/*
 * Apply substitution on filename received.
 */
const char *
userfn_subst(const char *fn)
{
	struct psc_thread *thr;
	struct recvthr *rt;
	const char *s = fn;
	char *t;

	thr = pscthr_get();
	rt = thr->pscthr_private;
	t = rt->fnbuf;
	if (*s == '~') {
		struct passwd pw, *res = NULL;
		int bufsz, rc;
		char *pwbuf;

		bufsz = sysconf(_SC_GETPW_R_SIZE_MAX);
		if (bufsz == -1)
			err(1, "sysconf");

		pwbuf = PSCALLOC(bufsz);

		s++;
		if (*s == '/' || *s == '\0') {
			/* expand current user */
			getpwuid_r(geteuid(), &pw, pwbuf, bufsz, &res);
		} else {
			size_t len;
			char *nam;

			/* expand specified user */
			do
				s++;
			while (*s && *s != '/');

			len = s - fn;
			nam = PSCALLOC(len + 1);
			strncpy(nam, fn, len);
			nam[len] = '\0';
			getpwnam_r(nam, &pw, pwbuf, bufsz, &res);
			PSCFREE(nam);
		}
		if (res && (rc = snprintf(rt->fnbuf, sizeof(rt->fnbuf),
		    "%s", res->pw_dir)) != -1)
			t += rc;
		else
			s = fn;
		PSCFREE(pwbuf);
	}
	for (; *s && t < rt->fnbuf + sizeof(rt->fnbuf) - 1; s++, t++)
		*t = *s;
	*t = '\0';
	return (rt->fnbuf);
}

void
rpc_handle_putname(struct hdr *h, void *buf)
{
	int fd = -1, flags = 0;
	char objfn[PATH_MAX], ufn_buf[PATH_MAX];
	const char *ufn, *dir, *base, *orig_ufn;
	struct rpc_putname *pn = buf;
	struct stat stb;
	mode_t mode;

	// if (pn->dirlen > )
	//  EINVAL;

	orig_ufn = dir = pn->dir;
	base = pn->dir + pn->dirlen;

	if (stat(dir, &stb) == 0 && S_ISDIR(stb.st_mode)) {
		snprintf(ufn_buf, sizeof(ufn_buf), "%s/%s", dir, base);
		orig_ufn = ufn_buf;
	}

	/* apply incoming name substitutions */
	ufn = userfn_subst(orig_ufn);
psynclog_tdebug("USERFN [%lx] %s -> %s", h->xid, orig_ufn, ufn);

	if (S_ISCHR(pn->pstb.mode) ||
	    S_ISBLK(pn->pstb.mode)) {
		if (mknod(ufn, pn->pstb.mode, pn->pstb.rdev) == -1) {
			psynclog_warn("mknod %s", ufn);
			return;
		}
	} else if (S_ISDIR(pn->pstb.mode)) {
		if (mkdir(ufn, pn->pstb.mode) == -1 &&
		    errno != EEXIST) {
			psynclog_warn("mkdir %s", ufn);
			return;
		}
	} else if (S_ISFIFO(pn->pstb.mode)) {
		if (mkfifo(ufn, pn->pstb.mode) == -1) {
			psynclog_warn("mkfifo %s", ufn);
			return;
		}
	} else if (S_ISLNK(pn->pstb.mode)) {
		objns_makepath(objfn, pn->fid);
		if (symlink(objfn, ufn) == -1) {
			psynclog_warn("symlink %s", ufn);
			return;
		}
		flags |= AT_SYMLINK_NOFOLLOW;
	} else if (S_ISSOCK(pn->pstb.mode)) {
		struct sockaddr_un sun;

		fd = socket(AF_LOCAL, SOCK_STREAM, PF_UNSPEC);
		if (fd == -1) {
			psynclog_warn("socket %s", ufn);
			return;
		}
		memset(&sun, 0, sizeof(sun));
		sun.sun_family = AF_LOCAL;
		strlcpy(sun.sun_path, ufn, sizeof(sun.sun_path));
		SOCKADDR_SETLEN(&sun);
		if (bind(fd, (struct sockaddr *)&sun,
		    sizeof(sun)) == -1) {
			close(fd);
			psynclog_warn("bind %s", ufn);
			return;
		}
		close(fd);
		fd = -1;
	} else if (S_ISREG(pn->pstb.mode)) {
		objns_makepath(objfn, pn->fid);
psynclog_tdebug("objfn %s", objfn);
		fd = open(objfn, O_CREAT | O_RDWR, 0600);
		if (fd == -1) {
			psynclog_warn("open %s", ufn);
			return;
		}
psynclog_tdebug("ln %s -> %s", ufn, objfn);
		if (link(objfn, ufn) == -1) {
			psynclog_warn("link %s", ufn);
			return;
		}

		fcache_insert(pn->fid, fd);
	} else {
		psynclog_warn("invalid mode %#o", pn->pstb.mode);
		return;
	}

	if (opt_owner || opt_group)
		psync_chown(ufn, pn->pstb.uid, pn->pstb.gid, flags);

	mode = S_ISDIR(pn->pstb.mode) ? 0777 : 0666;
	if (opt_perms)
		mode = pn->pstb.mode;
	else if (opt_executability)
		mode |= pn->pstb.mode & _S_IXUGO;
	psync_chmod(ufn, mode & ~psync_umask, flags);

	if (opt_times)
		psync_utimes(ufn, pn->pstb.tim, flags);

	/* XXX BSD file flags */
	/* XXX MacOS setattrlist */
	/* XXX linux file attributes: FS_IOC_GETFLAGS */
	/* XXX extattr */
}

void
rpc_handle_done(struct hdr *h, void *buf)
{
	struct rpc_done *d = buf;

	(void)h;
	(void)buf;

	if (d->clean)
		psync_rm_objns = 1;
	psync_recv_finished = 1;
psynclog_tdebug("psync_recv_finished=1");
}

typedef void (*op_handler_t)(struct hdr *, void *);

op_handler_t ops[] = {
	rpc_handle_getfile_req,
	rpc_handle_getfile_rep,
	rpc_handle_putdata,
	rpc_handle_checkzero_req,
	rpc_handle_checkzero_rep,
	rpc_handle_getcksum_req,
	rpc_handle_getcksum_rep,
	rpc_handle_putname,
	rpc_handle_done
};

void
handle_signal(__unusedx int sig)
{
	exit_from_signal = 1;
}

void
recvthr_main(struct psc_thread *thr)
{
	void *buf = NULL;
	uint32_t bufsz = 0;
	struct recvthr *rt;
	struct hdr hdr;
	ssize_t rc;

	psc_atomic32_inc(&psync_nrecvthr);

	rt = thr->pscthr_private;
	while (pscthr_run(thr)) {
		rc = atomicio_read(rt->st->rfd, &hdr, sizeof(hdr));
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
		atomicio_read(rt->st->rfd, buf, hdr.msglen);

		if (exit_from_signal)
			break;
		ops[hdr.opc](&hdr, buf);

		if (exit_from_signal || psync_recv_finished)
			break;
	}

psynclog_tdebug("recv done, CLOSE %d", rt->st->rfd);
	close(rt->st->rfd);

	psc_atomic32_dec(&psync_nrecvthr);
}
