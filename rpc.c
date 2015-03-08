/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2015, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, modify, and distribute this software
 * for any purpose with or without fee is hereby granted, provided
 * that the above copyright notice and this permission notice
 * appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN NO EVENT SHALL
 * THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

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

	iov[2].iov_base = (void *)base;
	iov[2].iov_len = strlen(fn) + 1;

	psynclog_diag("send GETFILE_REQ xid=%#"PRIx64, xid);
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

	iov[0].iov_base = &pd;
	iov[0].iov_len = sizeof(pd);

	iov[1].iov_base = (void *)buf;
	iov[1].iov_len = len;

	psynclog_diag("send PUTDATA fid=%#"PRIx64" len=%zd", pd.fid,
	    len);
	stream_sendv(st, OPC_PUTDATA, iov, nitems(iov));
}

void
rpc_send_putname_req(struct stream *st, uint64_t fid, const char *fn,
    const struct stat *stb, const char *buf, int rflags)
{
	struct rpc_putname_req pn;
	struct iovec iov[3];
	int nio = 0;

	memset(&pn, 0, sizeof(pn));
	pn.flags = rflags;
	pn.fid = fid;
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

	stream_sendv(st, OPC_PUTNAME_REQ, iov, nio);
}

void
rpc_send_putname_rep(struct stream *st, uint64_t fid, int rc)
{
	struct rpc_putname_rep pnp;

	pnp.fid = fid;
	pnp.rc = rc;
	stream_send(st, OPC_PUTNAME_REP, &pnp, sizeof(pnp));
}

void
rpc_send_done(struct stream *st)
{
	stream_send(st, OPC_DONE, NULL, 0);
}

void
rpc_send_ready(struct stream *st)
{
	struct rpc_ready r;

	r.nstreams = opts.streams = getnstreams(
	    MIN(getnprocessors(), opts.streams));
	stream_send(st, OPC_READY, &r, sizeof(r));
}

#define LASTFIELDLEN(h, type) ((h)->msglen - sizeof(type))

/*
 * Handle a GETFILE request.  This simply instructs the puppet to
 * perform a PUTNAME + PUTDATA based on the parameters defined in this
 * request.
 */
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

		travflags = PFL_FILEWALKF_NOCHDIR;

		if (S_ISDIR(stb.st_mode) || base[0] == '\0') {
			/*
			 * If a directory was requested to be received,
			 * or no destination basename was specified, then
			 * we must fill in the basename based on the
			 * requested file set.
			 *
			 * `skip' advances pass the root of the dataset
			 * location so all files from the traversal are
			 * relative and exactly fit to their destination
			 * on the receiving host.
			 */
			if (opts.recursive)
				travflags |= PFL_FILEWALKF_RECURSIVE;
			p = strrchr(gfq->fn, '/');
			if (p)
				wa.skip = p - gfq->fn;
			else
				wa.skip = 0;
			wa.prefix = base[0] ? base : ".";
		} else {
			wa.skip = strlen(gfq->fn);
			wa.prefix = base;
		}
		wa.rflags = 0;

		gfp.rc = pfl_filewalk(gfq->fn, travflags, NULL,
		    push_putfile_walkcb, &wa);
	} else {
		gfp.rc = errno;
	}

	psynclog_diag("send GETFILE_REP rc=%d", gfp.rc);
	stream_send(st, OPC_GETFILE_REP, &gfp, sizeof(gfp));
}

void
rpc_handle_getfile_rep(struct stream *st, struct hdr *h, void *buf)
{
	struct rpc_getfile_rep *gfp = buf;

	//psc_atomic64_add(&nbytes_total, stb->st_size);
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

	thr = pscthr_get();
	rcvthr = thr->pscthr_private;

	len = h->msglen - sizeof(*pd);

	psynclog_diag("handle PUTDATA fid=%#"PRIx64, pd->fid);

	/*
	 * Optimization: there's a good chance this work unit is a later
	 * chunk of the same file as the last work unit we processed; so
	 * track the last file and use if appropriate instead of always
	 * searching anew.
	 */
	if (rcvthr->last_f && pd->fid == rcvthr->last_f->fid)
		f = rcvthr->last_f;
	else
		f = fcache_search(pd->fid);
	rc = pwrite(f->fd, pd->data, len, pd->off);
	if (rc != (ssize_t)len)
		psynclog_error("write off=%"PRId64" len=%zd "
		    "rc=%zd", pd->off, len, rc);

	/*
	 * As each thread still processes files in a serial fashion
	 * (it's just the file chunks that get scattered amongst the
	 * threads), whenever a `new' file is encountered, this thread
	 * is done with the old one, so drop our reference.
	 */
	if (rcvthr->last_f && pd->fid != rcvthr->last_f->fid)
		fcache_close(rcvthr->last_f);
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
rpc_handle_putname_req(struct stream *st, struct hdr *h, void *buf)
{
	char *sep, *ufn, objfn[PATH_MAX];
	struct rpc_putname_req *pn = buf;
	int rc = 0, fd = -1, flags = 0;
	mode_t mode;

	/* apply incoming name substitutions */
	ufn = userfn_subst(pn->fn);
	psynclog_diag("handle PUTNAME_REQ xid=%#"PRIx64" %s -> %s "
	    "mode=%0o flags=%d",
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
		/*
		 * The symbolic path is packed after the FS path in the
		 * RPC, so we advance past the FS path.
		 */
		if (symlink(pn->fn + strlen(pn->fn) + 1, ufn) == -1) {
			psynclog_warn("symlink %s -> %s", ufn, pn->fn +
			    strlen(pn->fn) + 1);
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
		struct stat dummy;
		int ntries = 0;

		objns_makepath(objfn, pn->fid);

		if (opts.partial && stat(ufn, &dummy) == 0) {
			/*
			 * It is OK to do this without worrying about
			 * racing because the master waits for our
			 * response to this RPC before sending file data
			 * when in --partial mode.
			 */
			if (link(ufn, objfn) == -1) {
				psynclog_warn("open %s", ufn);
				goto out;
			}
			fd = open(objfn, O_RDWR);
		} else
			fd = open(objfn, O_CREAT | O_RDWR, 0600);
		if (fd == -1) {
			rc = errno;
			psynclog_warn("open %s", ufn);
			goto out;
		}

		if (!opts.partial)
			unlink(ufn);

 retry:
		if (link(objfn, ufn) == -1) {
			/*
			 * Ugly race workaround hack: on some file
			 * systems, we seem to actually race between
			 * mkdirs() above and the dir actually being
			 * there when the create happens, so try a few
			 * times.  We should technically do this for all
			 * file types.
			 */
#define MAX_TRIES 5
			if (ntries++ < MAX_TRIES) {
				usleep(1000);
				goto retry;
			}
			rc = errno;
			close(fd);
			psynclog_warn("link %s -> %s", ufn, objfn);
			goto out;
		}
	} else {
		psynclog_warn("invalid mode %#o", pn->pstb.mode);
		return;
	}

	if (opts.owner || opts.group) {
		if (!opts.owner)
			pn->pstb.uid = -1;
		if (!opts.group)
			pn->pstb.gid = -1;
		psync_chown(ufn, pn->pstb.uid, pn->pstb.gid, flags);
	}

	mode = S_ISDIR(pn->pstb.mode) ? 0777 : 0666;
	if (opts.perms)
		mode = pn->pstb.mode;
	else if (opts.executability)
		mode |= pn->pstb.mode & _S_IXUGO;
	psync_chmod(ufn, mode & ~psync_umask, flags);

	if (opts.times) {
		if (S_ISREG(pn->pstb.mode)) {
			struct file *f;

			f = fcache_search(pn->fid);
			memcpy(f->tim, pn->pstb.tim, sizeof(f->tim));
			fcache_close(f);
		} else
			psync_utimes(ufn, pn->pstb.tim, flags);
	}

	/* XXX BSD file flags */
	/* XXX MacOS setattrlist */
	/* XXX linux file attributes: FS_IOC_GETFLAGS */
	/* XXX extattr */

	if (fd != -1)
		close(fd);

 out:
	if (opts.partial)
		rpc_send_putname_rep(st, pn->fid, rc);
}

void
rpc_handle_putname_rep(__unusedx struct stream *st,
    __unusedx struct hdr *h, void *buf)
{
	struct rpc_putname_rep *pnp = buf;
	struct filehandle *fh;

	fh = filehandle_search(pnp->fid);
	if (fh)
		psc_compl_ready(&fh->cmpl, pnp->rc);
}

void
rpc_handle_done(struct stream *st, __unusedx struct hdr *h,
    __unusedx void *buf)
{
	psynclog_diag("handle DONE");
	st->done = 1;
}

void
rpc_handle_ready(__unusedx struct stream *st, __unusedx struct hdr *h,
    void *buf)
{
	struct rpc_ready *r = buf;

	psynclog_diag("handle READY");
	if (r->nstreams > 0 &&
	    r->nstreams < opts.streams)
		opts.streams = r->nstreams;
	psc_compl_ready(&psync_ready, 1);
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
	rpc_handle_putname_req,
	rpc_handle_putname_rep,
	rpc_handle_done,
	rpc_handle_ready
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
		if (exit_from_signal || st->done)
			break;
	}

	psynclog_diag("rcvthr done, close fd=%d", st->rfd);
	close(st->rfd);

	spinlock(&rcvthrs_lock);
	psc_dynarray_removeitem(&rcvthrs, thr);
	freelock(&rcvthrs_lock);

	psc_compl_ready(&psync_ready, -1);
}
