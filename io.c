/*
 * %ISC_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2014-2016, Google, LLC
 * Copyright 2014-2015, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the
 * above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN NO EVENT SHALL THE
 * AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
 * DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR
 * PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
 * TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 * --------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * The I/O routines here are for writing data to files on the local file
 * system that was received via RPCs.
 */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/alloc.h"
#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "pfl/waitq.h"
#include "pfl/walk.h"

#include "psync.h"
#include "options.h"

struct psc_hashtbl	 fcache;

void
objns_create(void)
{
	static psc_spinlock_t lock = SPINLOCK_INIT;

	spinlock(&lock);
	if (objns_path[0] == '\0') {
		snprintf(objns_path, sizeof(objns_path), ".psync.%d",
		    opts.puppet);
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

void
_fcache_found(void *p, __unusedx void *arg)
{
	struct file *f = p;

	spinlock(&f->lock);
	f->refcnt++;
}

struct file *
fcache_search(uint64_t fid)
{
	struct psc_hashbkt *b;
	struct file *f;

	f = psc_hashtbl_search_cb(&fcache, _fcache_found, NULL, &fid);
	if (f)
		goto out;

	b = psc_hashbkt_get(&fcache, &fid);
	f = psc_hashbkt_search_cb(&fcache, b, _fcache_found, NULL,
	    &fid);
	if (f == NULL) {
		char fn[PATH_MAX];

		//f = psc_pool_get(file_pool);
		f = PSCALLOC(sizeof(*f));
		INIT_SPINLOCK(&f->lock);
		f->refcnt = 1;
		spinlock(&f->lock);
		psc_hashent_init(&fcache, f);
		f->fid = fid;

		objns_makepath(fn, fid);
		f->fd = open(fn, O_RDWR | O_CREAT, 0600);
		if (f->fd == -1)
			psync_fatal("%s", fn);

		psc_hashbkt_add_item(&fcache, b, f);
	}
	psc_hashbkt_put(&fcache, b);

 out:
	freelock(&f->lock);
	return (f);
}

void
fcache_close(struct file *f)
{
	struct psc_hashbkt *b;

	b = psc_hashent_getbucket(&fcache, f);
	spinlock(&f->lock);
	if (--f->refcnt == 0 && (f->flags & (FF_SAWLAST | FF_LINKED)) ==
	    (FF_SAWLAST | FF_LINKED) && f->nchunks == f->nchunks_seen) {
		char objfn[PATH_MAX];

		objfn[0] = '\0';

		psc_hashent_remove(&fcache, f);
		psynclog_diag("close fd=%d", f->fd);
		if (opts.times) {
			if (objfn[0] == '\0')
				objns_makepath(objfn, f->fid);
			psync_utimes(objfn, f->tim, 0);
		}
		if (objfn[0] == '\0')
			objns_makepath(objfn, f->fid);
		psync_chmod(objfn, f->mode, 0);
		close(f->fd);
		PSCFREE(f);
	} else
		freelock(&f->lock);

	psc_hashbkt_put(&fcache, b);
}

void
fcache_init(void)
{
	/*
	 * This hash table needs to dynamically resize.  The worst
	 * situation would be transferring many small files (~4KiB) over
	 * a 100Gbps pipe, which would require ~3 million open files and
	 * ~10 million buckets.
	 */
	psc_hashtbl_init(&fcache, 0, struct file, fid, hentry, 191,
	    NULL, "fcache");
}

int
objns_rm_cb(FTSENT *f, __unusedx void *arg)
{
	switch (f->fts_info) {
	case FTS_DP:
		if (rmdir(f->fts_path) == -1)
			warn("rmdir %s", f->fts_path);
		break;
	case FTS_F:
		if (unlink(f->fts_path) == -1)
			warn("unlink %s", f->fts_path);
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

	if (objns_path[0]) {
		/* unlink object namespace */
		pfl_filewalk(objns_path, PFL_FILEWALKF_RECURSIVE |
		    PFL_FILEWALKF_NOCHDIR, NULL, objns_rm_cb, NULL);
	}
}
