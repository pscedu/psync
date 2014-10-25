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
#include "pfl/fmt.h"
#include "pfl/mkdirs.h"
#include "pfl/net.h"
#include "pfl/pool.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/sys.h"
#include "pfl/thread.h"
#include "pfl/walk.h"

#include "options.h"
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
rpc_send_getfile(struct stream *st, uint64_t xid, const char *fn,
    const char *base)
{
	struct rpc_getfile_req gfq;
	struct iovec iov[3];

	memset(&gfq, 0, sizeof(gfq));

	iov[0].iov_base = &gfq;
	iov[0].iov_len = sizeof(gfq);

	iov[1].iov_base = (void *)fn;
	iov[1].iov_len = gfq.len = strlen(fn) + 1;
psynclog_tdebug("SEND GETFILE %lx", xid);

	iov[2].iov_base = (void *)base;
	iov[2].iov_len = strlen(fn) + 1;

	stream_sendxv(st, xid, OPC_GETFILE_REQ, iov, nitems(iov));
}

void
rpc_send_putdata(struct stream *st, uint64_t fid, off_t off,
    const void *buf, size_t len)
{
	struct rpc_putdata pd;
	struct iovec iov[2];

	memset(&pd, 0, sizeof(pd));
	pd.fid = fid;
	pd.off = off;
//psynclog_tdebug("PUT %lx", pd.fid);

	iov[0].iov_base = &pd;
	iov[0].iov_len = sizeof(pd);

	iov[1].iov_base = (void *)buf;
	iov[1].iov_len = len;

	stream_sendv(st, OPC_PUTDATA, iov, nitems(iov));
}

void
rpc_send_putname(struct stream *st, const char *fn,
    const struct stat *stb, const char *buf, int rflags)
{
	struct rpc_putname pn;
	struct iovec iov[3];
	int nio = 0;

	memset(&pn, 0, sizeof(pn));
	pn.flags = rflags;
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

	iov[nio].iov_base = &pn;
	iov[nio].iov_len = sizeof(pn);
	nio++;

	iov[nio].iov_base = (void *)fn;
	iov[nio].iov_len = strlen(fn) + 1;
	nio++;

	if (buf) {
		iov[nio].iov_base = (void *)buf;
		iov[nio].iov_len = strlen(buf) + 1;
		nio++;
	}

	stream_sendv(st, OPC_PUTNAME, iov, nio);
}

#define LASTFIELDLEN(h, type) ((h)->msglen - sizeof(type))

void
rpc_handle_getfile_req(struct stream *st, __unusedx struct hdr *h,
    void *buf)
{
	struct rpc_getfile_req *gfq = buf;
	struct rpc_getfile_rep gfp;
	struct walkarg wa;
	struct stat stb;
	int travflags;
	char *base;

	base = gfq->fn + gfq->len;

	if (stat(gfq->fn, &stb) == 0) {
		char *p;

		travflags = PFL_FILEWALKF_RELPATH;

		p = strrchr(gfq->fn, '/');
		if (p)
			wa.skip = p - gfq->fn;
		else
			wa.skip = 0;
		wa.prefix = base[0] ? base : ".";
		wa.rflags = 0;
		if (S_ISDIR(stb.st_mode) && opts.recursive)
			travflags |= PFL_FILEWALKF_RECURSIVE;

		gfp.rc = pfl_filewalk(gfq->fn, travflags, NULL,
		    push_putfile_walkcb, &wa);
	} else {
		gfp.rc = errno;
psynclog_tdebug("getfile %d", errno);
	}

	stream_send(st, OPC_GETFILE_REP, &gfp, sizeof(gfp));
}

void
rpc_handle_getfile_rep(struct stream *st, struct hdr *h, void *buf)
{
	struct rpc_getfile_rep *gfp = buf;

	(void)st;
	(void)h;
	(void)gfp;
}

void
rpc_handle_putdata(__unusedx struct stream *st, struct hdr *h,
    void *buf)
{
	struct rpc_putdata *pd = buf;
	struct psc_thread *thr;
	struct rcvthr *rcvthr;
	struct file *f;
	ssize_t rc;
	size_t len;

	len = h->msglen - sizeof(*pd);

//psynclog_tdebug("HANDLE PUT %lx", pd->fid);
	f = fcache_search(pd->fid); // XXX check rcvthr->last_f first
	rc = pwrite(f->fd, pd->data, len, pd->off);
	if (rc != (ssize_t)len)
		psynclog_error("write off=%"PRId64" len=%"PRId64" "
		    "rc=%zd", pd->off, len, rc);

	thr = pscthr_get();
	rcvthr = thr->pscthr_private;
	if (rcvthr->last_f && pd->fid != rcvthr->last_f->fid)
{psynclog_tdebug("close %p", rcvthr->last_f);

		fcache_close(rcvthr->last_f);
}
	rcvthr->last_f = f;
}

void
rpc_handle_checkzero_req(struct stream *st, struct hdr *h, void *buf)
{
	struct rpc_checkzero_req *czq = buf;
	struct rpc_checkzero_rep czp;
	struct buf *bp;
	struct file *f;
	ssize_t rc;

	bp = buf_get(czq->len);

	f = fcache_search(czq->fid);
	rc = pread(f->fd, bp->buf, czq->len, czq->off);
	if (rc == -1)
		err(1, "read");
	if ((uint64_t)rc != czq->len)
		warnx("read: short I/O");
	czp.rc = pfl_memchk(bp->buf, 0, rc);

	buf_release(bp);

	stream_sendx(st, h->xid, OPC_CHECKZERO_REP, &czp, sizeof(czp));
}

void
rpc_handle_checkzero_rep(struct stream *st, struct hdr *h, void *buf)
{
	struct rpc_checkzero_rep *czp = buf;

	(void)st;
	(void)h;
	(void)czp;
}

void
rpc_handle_getcksum_req(struct stream *st, struct hdr *h, void *buf)
{
	gcry_md_hd_t hd;
	gcry_error_t gerr;
	struct rpc_getcksum_req *gcq = buf;
	struct rpc_getcksum_rep gcp;
	struct buf *bp;
	struct file *f;
	ssize_t rc;

	bp = buf_get(gcq->len);

	f = fcache_search(gcq->fid);
	rc = pread(f->fd, bp->buf, gcq->len, gcq->off);
	if (rc == -1)
		err(1, "read");
	if ((uint64_t)rc != gcq->len)
		warnx("read: short I/O");

	gerr = gcry_md_open(&hd, GCRY_MD_SHA256, 0);
	if (gerr)
		psync_fatalx("gcry_md_open: error=%d", gerr);
	gcry_md_write(hd, bp->buf, rc);
	memcpy(gcry_md_read(hd, 0), gcp.digest, ALGLEN);
	gcry_md_close(hd);

	buf_release(bp);

	stream_sendx(st, h->xid, OPC_GETCKSUM_REP, &gcp, sizeof(gcp));
}

void
rpc_handle_getcksum_rep(struct stream *st, struct hdr *h, void *buf)
{
	struct rpc_getcksum_rep *gcp = buf;

	(void)st;
	(void)h;
	(void)gcp;
}

/*
 * Apply substitution on filename received.
 */
char *
userfn_subst(const char *fn)
{
	struct psc_thread *thr;
	struct rcvthr *rcvthr;
	const char *s = fn;
	char *t;

	thr = pscthr_get();
	rcvthr = thr->pscthr_private;
	t = rcvthr->fnbuf;
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
		if (res && (rc = snprintf(rcvthr->fnbuf,
		    sizeof(rcvthr->fnbuf), "%s", res->pw_dir)) != -1)
			t += rc;
		else
			s = fn;
		PSCFREE(pwbuf);
	}
	for (; *s && t < rcvthr->fnbuf + sizeof(rcvthr->fnbuf) - 1;
	    s++, t++)
		*t = *s;
	*t = '\0';
	return (rcvthr->fnbuf);
}

void
rpc_handle_putname(__unusedx struct stream *st, struct hdr *h, void *buf)
{
	int fd = -1, flags = 0;
	char *sep, *ufn, objfn[PATH_MAX];
	struct rpc_putname *pn = buf;
	mode_t mode;
char *mkdirs_path = NULL;

	/* apply incoming name substitutions */
	ufn = userfn_subst(pn->fn);
psynclog_tdebug("USERFN [%lx] %s -> %s [%o] fl %d",
    h->xid, pn->fn, ufn, pn->pstb.mode, pn->flags);

	if (pn->flags & RPC_PUTNAME_F_TRYDIR) {
		struct stat stb;

		sep = strrchr(ufn, '/');
		if (sep)
			*sep = '\0';
		if (stat(ufn, &stb) == 0 && S_ISDIR(stb.st_mode))
			*sep = '/';
	} else {
		/*
		 * We might race with other threads so ensure the
		 * directory hierarchy is intact.
		 */
		sep = strrchr(ufn, '/');
		if (sep) {
			*sep = '\0';
psynclog_tdebug("mkdirs %s", ufn);
mkdirs_path = pfl_strdup(ufn);
			if (mkdirs(ufn, 0700) == -1 && errno != EEXIST)
				psynclog_error("mkdirs %s", ufn);
			*sep = '/';
		}
	}

	if (S_ISCHR(pn->pstb.mode) ||
	    S_ISBLK(pn->pstb.mode)) {
		if (mknod(ufn, pn->pstb.mode, pn->pstb.rdev) == -1) {
			psynclog_warn("mknod %s", ufn);
			return;
		}
	} else if (S_ISDIR(pn->pstb.mode)) {
psynclog_tdebug("MKDIR %s %0o", ufn, pn->pstb.mode);
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
		if (symlink(pn->fn + strlen(pn->fn) + 1, ufn) == -1) {
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
		char pbuf[11];

		objns_makepath(objfn, pn->fid);

		fd = open(objfn, O_CREAT | O_RDWR, 0600);
		if (fd == -1) {
			psynclog_warn("open %s", ufn);
			return;
		}
int ntry=0;
retry:
psynclog_tdebug("ln %s -> %s [%s] %d", ufn, objfn, pfl_fmt_mode(pn->pstb.mode, pbuf), flags);
		if (link(objfn, ufn) == -1) {
if (ntry++ < 5) {
	usleep(1000);
	goto retry;
}
			close(fd);

//pfl_systemf("ls -ld '%s' >&2", mkdirs_path);
//psynclog_warn("ln %s -> %s [%s] %d {%s}", ufn, objfn, pfl_fmt_mode(pn->pstb.mode, pbuf), flags, mkdirs_path);

			psynclog_warn("link %s -> %s", ufn, objfn);
			return;
		}
	} else {
		psynclog_warn("invalid mode %#o", pn->pstb.mode);
		return;
	}

	if (opts.owner || opts.group)
		psync_chown(ufn, pn->pstb.uid, pn->pstb.gid, flags);

	mode = S_ISDIR(pn->pstb.mode) ? 0777 : 0666;
	if (opts.perms)
		mode = pn->pstb.mode;
	else if (opts.executability)
		mode |= pn->pstb.mode & _S_IXUGO;
	psync_chmod(ufn, mode & ~psync_umask, flags);

	if (opts.times)
		psync_utimes(ufn, pn->pstb.tim, flags);

	/* XXX BSD file flags */
	/* XXX MacOS setattrlist */
	/* XXX linux file attributes: FS_IOC_GETFLAGS */
	/* XXX extattr */

	if (fd != -1)
		close(fd);
	//fcache_insert(pn->fid, fd);
}

typedef void (*op_handler_t)(struct stream *, struct hdr *, void *);

op_handler_t ops[] = {
	rpc_handle_getfile_req,
	rpc_handle_getfile_rep,
	rpc_handle_putdata,
	rpc_handle_checkzero_req,
	rpc_handle_checkzero_rep,
	rpc_handle_getcksum_req,
	rpc_handle_getcksum_rep,
	rpc_handle_putname
};

void
handle_signal(__unusedx int sig)
{
	exit_from_signal = 1;
}

void
rcvthr_main(struct psc_thread *thr)
{
	void *buf = NULL;
	uint32_t bufsz = 0;
	struct rcvthr *rcvthr;
	struct stream *st;
	struct hdr hdr;
	ssize_t rc;

	spinlock(&rcvthrs_lock);
	push(&rcvthrs, thr);
	freelock(&rcvthrs_lock);

	rcvthr = thr->pscthr_private;
	st = rcvthr->st;
	while (pscthr_run(thr)) {
		rc = atomicio_read(st->rfd, &hdr, sizeof(hdr));
		if (rc == 0)
			break;
		if (exit_from_signal)
			break;

		if (hdr.msglen > bufsz) {
			if (hdr.msglen > MAX_BUFSZ)
				psync_fatalx("invalid bufsz received "
				    "from peer: %u", hdr.msglen);
			bufsz = hdr.msglen;
			buf = realloc(buf, bufsz);
			if (buf == NULL)
				err(1, NULL);
		}
		if (hdr.opc >= nitems(ops))
			psync_fatalx("invalid opcode received from "
			    "peer: %u", hdr.opc);
		atomicio_read(st->rfd, buf, hdr.msglen);

		if (exit_from_signal)
			break;
		ops[hdr.opc](st, &hdr, buf);
		if (exit_from_signal)
			break;
	}

	close(st->rfd);
psynclog_tdebug("recv done, CLOSE %d", st->rfd);

	spinlock(&rcvthrs_lock);
	psc_dynarray_removeitem(&rcvthrs, thr);
	freelock(&rcvthrs_lock);
}
