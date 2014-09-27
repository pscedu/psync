/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct file {
	struct psc_hashent      hentry;
	uint64_t		fid;
	int			fd;

};


struct psc_hashtbl	 fcache;


void
objns_makepath(char *fn, uint64_t fid)
{
	static char objns_tab[] = "0123456789abcdef";

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

struct xid_mapping {
	struct psc_hashent       hentry;
	uint64_t		 xid;
	const char		*fn;
};

void
xm_insert(uint64_t xid, const char *fn)
{
	struct xid_mapping *xm;

	xm = PSCALLOC(sizeof(*xm));
	psc_hashent_init(&xmcache, xm);
	xm->xid = xid;
	xm->fn = pfl_strdup(fn);
	psc_hashtbl_add_item(&xmcache, xm);
}

#define fcache_search(fid)	_fcache_search((fid), -1)
#define fcache_insert(fid, fd)	_fcache_search((fid), (fd))

int
_fcache_search(uint64_t fid, int fd)
{
	struct psc_hashbkt *b;
	struct file *f;

	f = pfl_hashtbl_search(&fcache, NULL, NULL, &fid);
	if (f)
		goto out;

	b = pfl_hashbkt_get(&fcache, &fid);
	psc_hashbkt_lock(b);
	f = psc_hashbkt_search(&fcache, b, NULL, NULL, &fid);
	if (f == NULL) {
		f = PSCALLOC(sizeof(*f));
		psc_hashent_init(&fcache, f);
		f->fid = fid;

		if (fd == -1) {
			char fn[PATH_MAX];

			objns_makepath(fn, fid);
			f->fd = open(fn, O_RDWR | O_CREAT | O_EXCL, 0600);
			if (f->fd == -1)
				err(1, "%s", fn);
		} else {
			f->fd = fd;
		}
		fd = -1;

		psc_hashbkt_add_item(&fcache, b, f);
	}
	psc_hashbkt_unlock(b);
 out:
	if (fd != -1)
		close(fd);
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

void
objns_create(void)
{
	char fn[PATH_MAX];

	snprintf(objns, sizeof(objns), ".psync.%d",
	    psc_random32u(1000000));
	if (mkdir(objns, 0700) == -1)
		err(1, "mkdir %s", objns);
}

int
fcache_init(void)
{
	/*
	 * To saturate a 100Gb/sec pipe with 4k files, we need
	 * to send about 3 million in parallel...
	 */
	psc_hashtbl_init(&fcache, 0, struct file, fid, hentry, 191, NULL,
	    "fcache");
	psc_hashtbl_init(&xmcache, 0, struct xid_mapping, xid, hentry, 191,
	    NULL, "xmcache");
	objns_create();
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
fcache_destroy(void)
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