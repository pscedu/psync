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
struct psc_hashtbl	 fcache;

volatile sig_atomic_t	 exit_from_signal;

struct file {
	struct psc_hashent      hentry;
	uint64_t		fid;
	int			fd;

};

char objns_tab[] = "0123456789abcdef";

void
objns_makepath(char *fn, uint64_t fid)
{
	char *p;
	int i;

	i = snprintf(fn, PATH_MAX, "%s/", objns);
	/*
	 * ensure name fits into:
	 * <objns_dir>/abc/abcd0123abcd0123
	 * length + 1 + depth + 1 + 16
	 */
	if (i == -1 || i >= PATH_MAX - (objns_depth + 2 + 16))
		err(1, "snprintf");
	p = fn + i;

	/* create a path */
	for (i = 0; i < objns_depth; i++)
		*p++ = objns_tab[(fid >> (4 * (i + 2))) & 0xf];
	*p++ = '/';
	*p = '\0';

	if (mkdir(fn) == -1 && errno != EEXIST)
		err(1, "mkdir %s", fn);

	snprintf(p, PATH_MAX - (p - fn), "%016"PRIx64, fid);
}

int
fcache_search(uint64_t fid)
{
	struct psc_hashbkt *b;
	struct file *f;

	f = pfl_hashtbl_search(&fcache, NULL, NULL, &fid);
	if (f)
		return (f->fd);

	b = pfl_hashbkt_get(&fcache, &fid);
	psc_hashbkt_lock(b);
	f = psc_hashbkt_search(&fcache, b, NULL, NULL, &fid);
	if (f == NULL) {
		char fn[PATH_MAX];

		f = PSCALLOC(sizeof(*f));
		psc_hashent_init(&fcache, f);
		f->fid = fid;

		objns_makepath(fn, fid);
		f->fd = open(fn, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (f->fd == -1)
			err(1, "%s", fn);

		psc_hashbkt_add_item(&fcache, b, f);

	}
	psc_hashbkt_unlock(b);
	return (f->fd);
}

void
fcache_close(uint64_t fid)
{
	struct file *f;

	f = psc_hashtbl_searchdel(&fcache, NULL, &fid);
	if (f) {
		close(f->fd);
		PSCFREE(f);
	}
}

int
fcache_init(void)
{
	/*
	 * To saturate a 100Gb/sec pipe with 4k files, we need
	 * to send about 3 million in parallel...
	 */
	psc_hashtbl_init(&fbtbl, 0, struct file, fid, hentry,
	    29989, NULL, "fcache");
}

int
objns_rm_cb(const char *fn, __unusedx const struct pfl_stat *stb,
    int ftyp, __unusedx int level, __unusedx void *arg)
{
	switch (ftyp) {
	case PFWT_D:
		break;
	case PFWT_DP:
		if (rmdir(fn) == -1)
			warn("rmdir %s", fn);
		break;
	case PFWT_F:
		if (unlink(fn) == -1)
			warn("unlink %s", fn);
		break;
	}
	return (0);
}

void
fcache_closeall(void)
{
	struct psc_hashbkt *b;
	struct file *f, *fn;

	PSC_HASHTBL_FOREACH_BUCKET(b, &fcache)
		PSC_HASHBKT_FOREACH_ENTRY_SAFE(&fcache, f, fn, b) {
			close(f->fd);
			psc_hashbkt_del_item(&fcache, b, f);
			PSCFREE(f);
		}

	/* unlink object namespace */
	pfl_walkfiles(objns, PFL_FILEWALKF_RECURSIVE, objns_rm_cb, NULL);
}

int
getfile_cb(const char *fn, const struct stat *stb, void *arg)
{
	static struct rpc_pushname *m;
	static size_t len;

	m;

	stream_send(stream, OPC_PUSHNAME, &m, sizeof(m));
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
rpc_handle_getcrc(struct hdr *h, void *buf)
{
	struct rpc_getcrc_req *cq = buf;
	struct rpc_getcrc_rep *cp;
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

void
objns_create(void)
{
	char fn[PATH_MAX];

	snprintf(objns, sizeof(objns), ".psync.%d",
	    psc_random32u(1000000));
	if (mkdir(objns, 0700) == -1)
		err(1, "mkdir %s", objns);
}

void
rpc_handle_putname(struct hdr *h, void *buf)
{
	struct rpc_putname *pn = buf;

	fd = fcache_search(cq->fid);
	if (fd) {
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
	rpc_handle_getcrc,
	rpc_handle_putname,
	rpc_handle_ctl
};

void
handle_signal(__unusedx int sig)
{
	exit_from_signal = 1;
}

int
puppet_mode(int id)
{
	uint32_t bufsz = 0;
	ssize_t rc;
	void *buf;
	int rfd

	stream.rfd = rfd = STDIN_FILENO;
	stream.wfd = STDOUT_FILENO;

	signal(SIGINT, handle_signal);
	signal(SIGPIPE, handle_signal);

	fcache_init();
	objns_create();

	while (atomic_read(rfd, &hdr, sizeof(hdr))) {
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
	fcache_closeall();
	return (0);
}
