/* $Id$ */

#include <sys/param.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/alloc.h"
#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "pfl/walk.h"

#include "psync.h"

struct psc_hashtbl	 fcache;

void
objns_create(void)
{
	static psc_spinlock_t lock = SPINLOCK_INIT;

	spinlock(&lock);
	if (objns_path[0] == '\0') {
		snprintf(objns_path, sizeof(objns_path), ".psync.%d",
		    opt_puppet);
		if (mkdir(objns_path, 0700) == -1 && errno != EEXIST)
			psync_fatal("mkdir %s", objns_path);
	}
	freelock(&lock);
}

void
objns_makepath(char *fn, uint64_t fid)
{
	static char objns_tab[] = "0123456789abcdef";
	static int init;

	char *p;
	int i;

	if (!init) {
		objns_create();
		init = 1;
	}

	i = snprintf(fn, PATH_MAX, "%s/", objns_path);
	/*
	 * ensure name fits into:
	 * <objns_dir>/abc/abcd0123abcd0123
	 * length + 1 + depth + 1 + 16
	 */
	if (i == -1 || i >= PATH_MAX - (objns_depth + 2 + 16))
		psync_fatal("snprintf");
	p = fn + i;

	/* create a path */
	for (i = 0; i < objns_depth; i++)
		*p++ = objns_tab[(fid >> (4 * (i + 2))) & 0xf];
	*p++ = '/';
	*p = '\0';

	/* XXX could use a bitmap to skip this */

	if (mkdir(fn, 0700) == -1 && errno != EEXIST)
		psync_fatal("mkdir %s", fn);

	snprintf(p, PATH_MAX - (p - fn), "%016"PRIx64, fid);
}

int
_fcache_search(uint64_t fid, int fd)
{
	struct psc_hashbkt *b;
	struct file *f;

	f = psc_hashtbl_search(&fcache, NULL, NULL, &fid);
	if (f)
		return (f->fd);

	b = psc_hashbkt_get(&fcache, &fid);
	f = psc_hashbkt_search(&fcache, b, NULL, NULL, &fid);
	if (f == NULL) {
		f = PSCALLOC(sizeof(*f));
		psc_hashent_init(&fcache, f);
		f->fid = fid;

		if (fd == -1) {
			char fn[PATH_MAX];

			objns_makepath(fn, fid);
			f->fd = open(fn, O_RDWR | O_CREAT, 0600);
			if (f->fd == -1)
				psync_fatal("%s", fn);
		} else {
			f->fd = fd;
			fd = -1;
		}

		psc_hashbkt_add_item(&fcache, b, f);
	}
	psc_hashbkt_put(&fcache, b);

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
psynclog_debug("CLOSE %d\n", f->fd);
		close(f->fd);
		/* XXX refcnting/race ?? */
		PSCFREE(f);
	}
}

void
fcache_init(void)
{
	/*
	 * To saturate a 100Gb/sec pipe with 4k files, we need
	 * to send about 3 million in parallel...
	 */
	psc_hashtbl_init(&fcache, 0, struct file, fid, hentry, 191,
	    NULL, "fcache");
}

int
objns_rm_cb(const char *fn, __unusedx const struct stat *stb,
    int ftyp, __unusedx int level, __unusedx void *arg)
{
	switch (ftyp) {
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
psynclog_debug("CLOSE %d\n", f->fd);
			close(f->fd);
			psc_hashbkt_del_item(&fcache, b, f);
			PSCFREE(f);
		}

	if (psync_rm_objns && objns_path[0]) {
		/* unlink object namespace */
		pfl_filewalk(objns_path, PFL_FILEWALKF_RECURSIVE, NULL,
		    objns_rm_cb, NULL);
	}
}
