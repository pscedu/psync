/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <ctype.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/fmt.h"
#include "pfl/iostats.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/pfl.h"
#include "pfl/pool.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/types.h"
#include "pfl/walk.h"

#include "psync.h"
#include "rpc.h"

#define MODE_GET	0
#define MODE_PUT	1

struct work {
	struct psc_listentry	  wk_lentry;
	char			  wk_fn[PATH_MAX];
	char			  wk_basefn[NAME_MAX + 1];
	struct filehandle	 *wk_fh;
	char			  wk_host[HOST_NAME_MAX + 1];
	int			  wk_type;
	size_t			  wk_len;
	struct stat		  wk_stb;
	uint64_t		  wk_xid;
	off_t			  wk_off;
	void			(*wk_cb)(struct work *);
};

enum {
	THRT_DISP,
	THRT_MAIN,
	THRT_RECV,
	THRT_TIOS,
	THRT_WK
};

const char		*progname;

const char		*opt_address;
uint64_t		 opt_block_size;
const char		*opt_chmod;
const char		*opt_compare_dest;
const char		*opt_copy_dest;
const char		*opt_link_dest;
const char		*opt_log_file;
const char		*opt_log_file_format;
int			 opt_max_delete;
uint64_t		 opt_max_size;
uint64_t		 opt_min_size;
int			 opt_modify_window;
const char		*opt_out_format;
const char		*opt_partial_dir;
const char		*opt_password_file;
const char		*opt_psync_path = "psync";
const char		*opt_read_batch;
const char		*opt_rsh = "ssh -oControlPath=none";
const char		*opt_sockopts;
const char		*opt_suffix;
const char		*opt_temp_dir;
const char		*opt_write_batch;
int			 opt_8_bit_output;
int			 opt_append;
int			 opt_backup;
int			 opt_blocking_io;
uint64_t		 opt_bwlimit;
int			 opt_cache;
int			 opt_checksum;
int			 opt_compress;
int			 opt_compress_level;
int			 opt_copy_dirlinks;
int			 opt_copy_links;
int			 opt_copy_unsafe_links;
int			 opt_cvs_exclude;
int			 opt_del;
int			 opt_delay_updates;
int			 opt_delete;
int			 opt_delete_after;
int			 opt_delete_before;
int			 opt_delete_during;
int			 opt_delete_excluded;
int			 opt_devices;
int			 opt_dirs;
int			 opt_dry_run;
int			 opt_excutability;
int			 opt_existing;
int			 opt_extended_attributes;
int			 opt_force;
int			 opt_from0;
int			 opt_fuzzy;
int			 opt_group;
int			 opt_hard_links;
int			 opt_human_readable;
int			 opt_ignore_errors;
int			 opt_ignore_existing;
int			 opt_ignore_times;
int			 opt_inplace;
int			 opt_ipv4;
int			 opt_ipv6;
int			 opt_itemize_changes;
int			 opt_keep_dirlinks;
int			 opt_links;
int			 opt_list_only;
int			 opt_no_implied_dirs;
int			 opt_numeric_ids;
int			 opt_omit_dir_times;
int			 opt_one_file_system;
int			 opt_owner;
int			 opt_partial;
int			 opt_perms;
int			 opt_port;
int			 opt_progress = 1;
int			 opt_prune_empty_dirs;
int			 opt_puppet;
int			 opt_quiet;
int			 opt_recursive;
int			 opt_relative;
int			 opt_remove_source_file;
int			 opt_remove_source_files;
int			 opt_safe_links;
int			 opt_size_only;
int			 opt_sparse;
int			 opt_specials;
int			 opt_stats;
int			 opt_streams = 1;
int			 opt_super;
int			 opt_timeout;		/* in seconds */
int			 opt_times;
int			 opt_update;
int			 opt_verbose;
int			 opt_whole_file;

struct psc_dynarray	 opt_exclude = DYNARRAY_INIT;
struct psc_dynarray	 opt_files = DYNARRAY_INIT;
struct psc_dynarray	 opt_filter = DYNARRAY_INIT;
struct psc_dynarray	 opt_include = DYNARRAY_INIT;

struct psc_poolmaster	 buf_poolmaster;
struct psc_poolmgr	*buf_pool;

struct psc_listcache	 workq;
struct psc_poolmaster	 work_poolmaster;
struct psc_poolmgr	*work_pool;
pthread_barrier_t	 work_barrier;

struct psc_iostats	 iostats;

struct psc_dynarray	 streams = DYNARRAY_INIT;

int			 psync_is_master;	/* otherwise, is RPC puppet */
int			 psync_finished;

#define NO_ARG		no_argument
#define REQARG		required_argument

enum {
	OPT_ADDRESS = 'z' + 1,
	OPT_BWLIMIT,
	OPT_CHMOD,
	OPT_COMPARE_DEST,
	OPT_COMPRESS_LEVEL,
	OPT_COPY_DEST,
	OPT_EXCLUDE,
	OPT_EXCLUDE_FROM,
	OPT_FILES_FROM,
	OPT_INCLUDE,
	OPT_INCLUDE_FROM,
	OPT_LINK_DEST,
	OPT_LOG_FILE,
	OPT_LOG_FILE_FORMAT,
	OPT_MAX_DELETE,
	OPT_MAX_SIZE,
	OPT_MIN_SIZE,
	OPT_MODIFY_WINDOW,
	OPT_ONLY_WRITE_BATCH,
	OPT_OUT_FORMAT,
	OPT_PARTIAL_DIR,
	OPT_PASSWORD_FILE,
	OPT_PORT,
	OPT_PSYNC_PATH,
	OPT_READ_BATCH,
	OPT_SOCKOPTS,
	OPT_SUFFIX,
	OPT_TIMEOUT,
	OPT_WRITE_BATCH
};

struct option opts[] = {
	{ "8-bit-output",	NO_ARG,	NULL,			'8' },
	{ "PUPPET",		NO_ARG,	&opt_puppet,		1 },
	{ "address",		REQARG,	NULL,			OPT_ADDRESS },
	{ "append",		NO_ARG,	&opt_append,		1 },
	{ "archive",		NO_ARG,	NULL,			'a' },
	{ "backup",		NO_ARG,	NULL,			'b' },
	{ "backup-dir",		NO_ARG,	NULL,			1 },
	{ "block-size",		REQARG,	NULL,			'B' },
	{ "blocking-io",	NO_ARG,	&opt_blocking_io,	1 },
	{ "bwlimit",		REQARG,	NULL,			OPT_BWLIMIT },
	{ "cache",		NO_ARG,	&opt_cache,		1 },
	{ "checksum",		NO_ARG,	NULL,			'c' },
	{ "chmod",		REQARG,	NULL,			OPT_CHMOD },
	{ "compare-dest",	REQARG,	NULL,			OPT_COMPARE_DEST },
	{ "compress",		NO_ARG,	NULL,			'z' },
	{ "compress-level",	REQARG,	NULL,			OPT_COMPRESS_LEVEL },
	{ "copy-dest",		REQARG,	NULL,			OPT_COPY_DEST },
	{ "copy-dirlinks",	NO_ARG,	NULL,			'k' },
	{ "copy-links",		NO_ARG,	NULL,			'L' },
	{ "copy-unsafe-links",	NO_ARG,	&opt_copy_unsafe_links,	1 },
	{ "cvs-exclude",	NO_ARG,	NULL,			'C' },
	{ "del",		NO_ARG,	&opt_del,		1 },
	{ "delay-updates",	NO_ARG,	&opt_delay_updates,	1 },
	{ "delete",		NO_ARG,	&opt_delete,		1 },
	{ "delete-after",	NO_ARG,	&opt_delete_after,	1 },
	{ "delete-before",	NO_ARG,	&opt_delete_before,	1 },
	{ "delete-during",	NO_ARG,	&opt_delete_during,	1 },
	{ "delete-excluded",	NO_ARG,	&opt_delete_excluded,	1 },
	{ "devices",		NO_ARG,	&opt_devices,		1 },
	{ "dirs",		NO_ARG,	NULL,			'd' },
	{ "dry-run",		NO_ARG,	NULL,			'n' },
	{ "exclude",		REQARG,	NULL,			OPT_EXCLUDE },
	{ "exclude-from",	REQARG,	NULL,			OPT_EXCLUDE_FROM },
	{ "executability",	NO_ARG,	&opt_excutability,	1 },
	{ "existing",		NO_ARG,	&opt_existing,		1 },
	{ "extended-attributes",NO_ARG,	NULL,			'E' },
	{ "files-from",		REQARG,	NULL,			OPT_FILES_FROM },
	{ "filter",		REQARG,	NULL,			'f' },
	{ "force",		NO_ARG,	&opt_force,		1 },
	{ "from0",		NO_ARG,	NULL,			'0' },
	{ "fuzzy",		NO_ARG,	NULL,			'y' },
	{ "group",		NO_ARG,	NULL,			'g' },
	{ "hard-links",		NO_ARG,	NULL,			'H' },
	{ "human-readable",	NO_ARG,	NULL,			'h' },
	{ "ignore-errors",	NO_ARG,	&opt_ignore_errors,	1 },
	{ "ignore-existing",	NO_ARG,	&opt_ignore_existing,	1 },
	{ "ignore-times",	NO_ARG,	NULL,			'I' },
	{ "include",		REQARG,	NULL,			OPT_INCLUDE },
	{ "include-from",	REQARG,	NULL,			OPT_INCLUDE_FROM },
	{ "inplace",		NO_ARG,	&opt_inplace,		1 },
	{ "ipv4",		NO_ARG,	NULL,			'4' },
	{ "ipv6",		NO_ARG,	NULL,			'6' },
	{ "itemize-changes",	NO_ARG,	NULL,			'i' },
	{ "keep-dirlinks",	NO_ARG,	NULL,			'K' },
	{ "link-dest",		REQARG,	NULL,			OPT_LINK_DEST },
	{ "links",		NO_ARG,	NULL,			'l' },
	{ "list-only",		NO_ARG,	&opt_list_only,		1 },
	{ "log-file",		REQARG,	NULL,			OPT_LOG_FILE },
	{ "log-file-format",	REQARG,	NULL,			OPT_LOG_FILE_FORMAT },
	{ "max-delete",		REQARG,	NULL,			OPT_MAX_DELETE },
	{ "max-size",		REQARG,	NULL,			OPT_MAX_SIZE },
	{ "min-size",		REQARG,	NULL,			OPT_MIN_SIZE },
	{ "modify-window",	REQARG,	NULL,			OPT_MODIFY_WINDOW },
	{ "no-implied-dirs",	NO_ARG,	&opt_no_implied_dirs,	1 },
	{ "numeric-ids",	NO_ARG,	&opt_numeric_ids,	1 },
	{ "omit-dir-times",	NO_ARG,	NULL,			'O' },
	{ "one-file-system",	NO_ARG,	NULL,			'x' },
	{ "only-write-batch",	REQARG,	NULL,			OPT_ONLY_WRITE_BATCH },
	{ "out-format",		REQARG,	NULL,			OPT_OUT_FORMAT },
	{ "owner",		NO_ARG,	NULL,			'o' },
	{ "partial",		NO_ARG,	&opt_partial,		1 },
	{ "partial-dir",	REQARG,	NULL,			OPT_PARTIAL_DIR },
	{ "password-file",	REQARG,	NULL,			OPT_PASSWORD_FILE },
	{ "perms",		NO_ARG,	NULL,			'p' },
	{ "port",		REQARG,	NULL,			OPT_PORT },
	{ "progress",		NO_ARG,	&opt_progress,		1 },
	{ "prune-empty-dirs",	NO_ARG,	NULL,			'm' },
	{ "psync-path",		REQARG,	NULL,			OPT_PSYNC_PATH },
	{ "quiet",		NO_ARG,	NULL,			'q' },
	{ "read-batch",		REQARG,	NULL,			OPT_READ_BATCH },
	{ "recursive",		NO_ARG,	NULL,			'r' },
	{ "relative",		NO_ARG,	NULL,			'R' },
	{ "remove-source-files",NO_ARG,	&opt_remove_source_files,1 },
	{ "rsh",		REQARG,	NULL,			'e' },
	{ "safe-links",		NO_ARG,	&opt_safe_links,	1 },
	{ "size-only",		NO_ARG,	&opt_size_only,		1 },
	{ "sockopts",		REQARG,	NULL,			OPT_SOCKOPTS },
	{ "sparse",		NO_ARG,	NULL,			'S' },
	{ "specials",		NO_ARG,	&opt_specials,		1 },
	{ "stats",		NO_ARG,	&opt_stats,		1 },
	{ "streams",		REQARG,	&opt_streams,		'N' },
	{ "suffix",		REQARG,	NULL,			OPT_SUFFIX },
	{ "super",		NO_ARG,	&opt_super,		1 },
	{ "temp-dir",		REQARG,	NULL,			'T' },
	{ "timeout",		REQARG,	NULL,			OPT_TIMEOUT },
	{ "times",		NO_ARG,	NULL,			't' },
	{ "update",		NO_ARG,	NULL,			'u' },
	{ "verbose",		NO_ARG,	NULL,			'v' },
	{ "version",		NO_ARG,	NULL,			'V' },
	{ "whole-file",		NO_ARG,	NULL,			'W' },
	{ "write-batch",	REQARG,	NULL,			OPT_WRITE_BATCH },
	{ NULL,			0,	NULL,			0 }
};

void
filehandle_dropref(struct filehandle *fh, size_t len)
{
	spinlock(&fh->lock);
	if (--fh->refcnt == 0 &&
	    fh->flags & FHF_DONE) {
		munmap(fh->base, len);
dbglog("CLOSE %d\n", fh->fd);
		close(fh->fd);
		PSCFREE(fh);
	} else
		freelock(&fh->lock);
}

void
proc_work(struct work *wk)
{
	switch (wk->wk_type) {
	case OPC_GETFILE_REQ:
		rpc_send_getfile(wk->wk_xid, wk->wk_fn);
		break;
	case OPC_PUTDATA:
		if (opt_sparse == 0 || !pfl_memchk(wk->wk_fh->base +
		    wk->wk_off, 0, wk->wk_len))
			rpc_send_putdata(wk->wk_stb.st_ino, wk->wk_off,
			    wk->wk_fh->base + wk->wk_off, wk->wk_len);
		filehandle_dropref(wk->wk_fh, wk->wk_stb.st_size);
		break;
	case OPC_PUTNAME:
		rpc_send_putname(wk->wk_fn, wk->wk_basefn, &wk->wk_stb);
		break;
	}
}

void
wkthr_main(struct psc_thread *thr)
{
	static psc_spinlock_t lock = SPINLOCK_INIT;
	struct stream *st;
	struct work *wk;
	int i;

	while (pscthr_run(thr)) {
		wk = lc_getwait(&workq);
		if (wk == NULL)
			break;
		wk->wk_cb(wk);
		psc_pool_return(work_pool, wk);

		if (exit_from_signal)
			break;
	}
dbglog("@@@@@@@@@ CLOSE ALL writefds");
	pthread_barrier_wait(&work_barrier);
	spinlock(&lock);
	if (!psync_finished) {
		DYNARRAY_FOREACH(st, i, &streams)
			close(st->wfd);
		psync_finished = 1;
	}
	freelock(&lock);
}

struct work *
work_getitem(int type)
{
	struct work *wk;

	wk = psc_pool_get(work_pool);
	memset(wk, 0, sizeof(*wk));
	INIT_LISTENTRY(&wk->wk_lentry);
	wk->wk_type = type;
	wk->wk_cb = proc_work;
	return (wk);
}

/*
 * @stb: stat(2) buffer only used during PUTs.
 */
void
enqueue_put(int mode, const char *srcfn, const char *orig_dstfn,
    const struct stat *stb)
{
	struct filehandle *fh;
	struct work *wk;
	off_t off = 0;
	size_t blksz;

	blksz = opt_block_size ? (blksize_t)opt_block_size :
	    stb->st_blksize;
blksz = 1;

	/* sending; push name first */
	wk = work_getitem(OPC_PUTNAME);
	memcpy(&wk->wk_stb, stb, sizeof(wk->wk_stb));
	strlcpy(wk->wk_basefn, pfl_basename(srcfn),
	    sizeof(wk->wk_basefn));
dbglog("DSTFN %s", wk->wk_basefn);
	strlcpy(wk->wk_fn, orig_dstfn, sizeof(wk->wk_fn));
dbglog("DSTDIR %s", wk->wk_fn);
	lc_add(&workq, wk);

	// S_ISREG()

	fh = PSCALLOC(sizeof(*fh));
	fh->fd = open(srcfn, O_RDONLY);
	if (fh->fd == -1)
		err(1, "%s", srcfn);

	INIT_SPINLOCK(&fh->lock);
	fh->base = mmap(NULL, stb->st_size, PROT_READ, MAP_FILE |
	    MAP_SHARED, fh->fd, 0);

	/* push data chunks */
	for (; off < stb->st_size; off += blksz) {
#if 0
		if (opt_partial) {
			if (checksum) {
				wk = work_getitem(OPC_GETCKSUM_REQ);
				wk->wk_off = off;
				wk->wk_len = ;
				lc_add(&workq, wk);
				return;
			}
			wk = work_getitem(OPC_GETSTAT_REQ);
			wk->wk_off = off;
			wk->wk_len = ;
			lc_add(&workq, wk);
			return;
		}
#endif

		wk = work_getitem(OPC_PUTDATA);
		wk->wk_fh = fh;

		spinlock(&fh->lock);
		fh->refcnt++;
		freelock(&fh->lock);

		wk->wk_off = off;
		if (off + (off_t)blksz > stb->st_size)
			wk->wk_len = stb->st_size % blksz;
		else
			wk->wk_len = blksz;
		lc_add(&workq, wk);
	}
	spinlock(&fh->lock);
	fh->flags |= FHF_DONE;
	psc_waitq_wakeall(&fh->wq);
	freelock(&fh->lock);
}

int
push_putfile_walkcb(const char *fn, const struct stat *stb,
    __unusedx int type, __unusedx int level, void *arg)
{
	struct walkarg *wa = arg;
	char *p, dstfn_buf[PATH_MAX];
	const char *dstfn;
	int rc = 0;

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

	if (wa->prefix) {
dbglog("fn %s", fn);
		snprintf(dstfn_buf, sizeof(dstfn_buf), "%s/%s",
		    wa->prefix, fn + wa->trim);
		p = strrchr(dstfn_buf, '/');
		*p = '\0';
		dstfn = dstfn_buf;
dbglog("PUT %s -> %s [%s]", fn, dstfn, wa->prefix);
	} else
		dstfn = fn;
	enqueue_put(MODE_PUT, fn, dstfn, stb);
	return (rc);
}

/*
 * put:
 *	psync file remote:dir/file
 *	psync file remote:dir/
 *	psync dir remote:dir/file
 *	psync dir remote:dir/
 * get:
 *	psync remote:file dir/file
 *	psync remote:file dir/
 *	psync remote:dir dir/file
 *	psync remote:dir dir/
 */
int
walkfiles(int mode, const char *srcfn, int flags, const char *dstfn)
{
	char buf[PATH_MAX];
	const char *finalfn;
	struct stat tstb;
	struct work *wk;

	if (mode == MODE_PUT) {
		struct walkarg wa;

		wa.trim = strlen(srcfn);
		wa.prefix = dstfn;
		return (pfl_filewalk(srcfn, flags, NULL,
		    push_putfile_walkcb, &wa));
	}

	/* otherwise, the operation is a FETCH */

	/*
	 * If destination is local and a directory, append
	 * remote source filename.
	 *
	 *	psync remote:file file
	 *	psync remote:file dir
	 *	psync remote:dir file
	 *	psync remote:dir dir
	 */
	finalfn = dstfn;
	if (stat(dstfn, &tstb) == 0 && S_ISDIR(tstb.st_mode)) {
		snprintf(buf, sizeof(buf), "%s/%s", finalfn,
		    pfl_basename(srcfn));
		finalfn = buf;
	}

	wk = work_getitem(OPC_GETFILE_REQ);
	wk->wk_xid = psc_atomic32_inc_getnew(&psync_xid);
dbglog("MAP %lx -> %s", wk->wk_xid, finalfn);
	strlcpy(wk->wk_fn, srcfn, sizeof(wk->wk_fn));
//	if (!opt_partial)
//		truncate(finalfn, 0);
	lc_add(&workq, wk);

	return (0);
}

void
pushfile(struct psc_dynarray *da, char *fn,
    void (*f)(struct psc_dynarray *, char *, int), int arg)
{
	char *p, buf[BUFSIZ];
	FILE *fp;

	fp = fopen(fn, "r");
	if (fp == NULL)
		err(1, "%s", fn);
	while (fgets(buf, sizeof(buf), fp)) {
		p = strdup(buf);
		if (p == NULL)
			err(1, NULL);
		f(da, p, arg);
	}
	if (ferror(fp))
		err(1, "%s", fn);
	fclose(fp);
}

struct filterpattern {
	int		 fp_type;
	int		 fp_flags;
	char		*fp_pat;
};

#define FPT_INCL	(1 << 0)
#define FPT_EXCL	(1 << 1)
#define FPT_MERGE	(1 << 2)
#define FPT_DIRMERGE	(1 << 3)
#define FPT_HIDE	(1 << 4)
#define FPT_SHOW	(1 << 5)
#define FPT_PROTECT	(1 << 6)
#define FPT_RISK	(1 << 7)
#define FPT_CLEAR	(1 << 8)

void
push_filter(struct psc_dynarray *da, char *s, int type)
{
	struct {
		const char	*name;
		const char	*abbr;
		int		 type;
	} *ty, types[] = {
		{ "clear",	"!", FPT_CLEAR },
		{ "dir-merge",	":", FPT_DIRMERGE },
		{ "exclude",	"-", FPT_EXCL },
		{ "hide",	"H", FPT_HIDE },
		{ "include",	"+", FPT_INCL },
		{ "merge",	".", FPT_MERGE },
		{ "protect",	"P", FPT_PROTECT },
		{ "risk",	"R", FPT_RISK },
		{ "show",	"S", FPT_SHOW }
	};
	struct filterpattern *fp;
	char *sty, *sep;
	int n;

	fp = PSCALLOC(sizeof(*fp));
	if (type) {
		fp->fp_type = type;
		fp->fp_pat = s;
	} else {
		for (sty = s; *s && !isspace(*s); s++)
			;
		while (isspace(*s))
			s++;
		if (*s == '\0')
			psc_fatal("error");
		sep = strchr(sty, ',');
		if (sep)
			*sep = '\0';

		for (n = 0, ty = types; n < nitems(types); ty++, n++)
			if (strcmp(ty->name, sty) == 0 ||
			    strcmp(ty->abbr, sty) == 0)
				break;
		if (n == nitems(types))
			psc_fatal("error");
		fp->fp_type = ty->type;
		fp->fp_pat = s;
	}
	push(da, fp);
}

void
push_files_from(struct psc_dynarray *da, char *fn,
    __unusedx int arg)
{
	push(da, fn);
}

int
filesfrom(int mode, const char *fromfn, int flags, const char *dstfn)
{
	char fn[PATH_MAX], *p = fn;
	int rc = 0, rv, c, lineno = 1;
	FILE *fp;

	fp = fopen(fromfn, "r");
	if (fp == NULL)
		err(1, "open %s", fromfn);
	for (;;) {
		c = fgetc(fp);
		if (c == EOF)
			break;
		if (c == '\n' || c == '\r') {
			lineno++;
			*p = '\0';
			if (p != fn) {
				rv = walkfiles(mode, fn, flags, dstfn);
				if (rv)
					rc = rv;
			}
			p = fn;
		} else {
			if (p == fn + sizeof(fn) - 1) {
				errno = ENAMETOOLONG;
				warn("%s:%d", fromfn, lineno);
			} else
				*p++ = c;
		}
	}
	fclose(fp);
	if (p != fn) {
		*p = '\0';
		rv = walkfiles(mode, fn, flags, dstfn);
		if (rv)
			rc = rv;
	}
	return (rc);
}

int
puppet_mode(void)
{
	struct psc_thread *thr, *wkthr;
	struct recvthr *rt;
	struct stream *st;

	signal(SIGINT, handle_signal);
	signal(SIGPIPE, handle_signal);

	st = stream_create(STDIN_FILENO, STDOUT_FILENO);
	psc_dynarray_add(&streams, st);

	wkthr = pscthr_init(THRT_WK, 0, wkthr_main, NULL, 0, "wkthr");

	/* XXX hack */
	thr = pscthr_get();
	rt = thr->pscthr_private = PSCALLOC(sizeof(*rt));
	rt->st = st;
	recvthr_main(thr);

	lc_kill(&workq);

	pthread_join(wkthr->pscthr_pthread, NULL);

	fcache_destroy();
	return (0);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s src dst\n", progname);
	exit(1);
}

void
dispthr_main(struct psc_thread *thr)
{
	char ratebuf[PSCFMT_HUMAN_BUFSIZ];
	struct psc_waitq wq = PSC_WAITQ_INIT;
	struct timespec ts;
	double d;

	PFL_GETTIMESPEC(&ts);
	ts.tv_nsec = 0;
	for (;;) {
		ts.tv_sec++;
		psc_waitq_waitabs(&wq, NULL, &ts);

		d = psc_iostats_getintvrate(&iostats, 0);

		psc_fmt_human(ratebuf, d);
		printf("%7s/s\r", ratebuf);
		fflush(stdout);
	}
}

int
main(int argc, char *argv[])
{
	int mode, flags, i, rv, rc = 0, c;
	char *p, *fn, *host, *dstfn;
	struct psc_dynarray threads = DYNARRAY_INIT;
	struct psc_thread *thr;

#if 0
	setenv("PSC_LOG_FORMAT", "%n: ", 0);
	setenv("PSC_LOG_LEVEL", "warn", 0);
#endif

	pfl_init();
	progname = argv[0];
	while ((c = getopt_long(argc, argv,
	    "0468aB:bCcdEEe:f:gHhIiKkLlmN:nOoPpqRrST:tuVvWxyz", opts,
	    NULL)) != -1) {
		switch (c) {
		case '0':		opt_from0 = 1;			break;
		case '4':		opt_ipv4 = 1;			break;
		case '6':		opt_ipv6 = 1;			break;
		case '8':		opt_8_bit_output = 1;		break;
		case 'a':		opt_devices = 1;
					opt_group = 1;
					opt_links = 1;
					opt_owner = 1;
					opt_perms = 1;
					opt_recursive = 1;
					opt_specials = 1;
					opt_times = 1;			break;
		case 'B':
			if (!parsesize(&opt_block_size, optarg, 1))
				err(1, "-B %s", optarg);
			break;
		case 'b':		opt_backup = 1;			break;
		case 'C':		opt_cvs_exclude = 1;		break;
		case 'c':		opt_checksum = 1;		break;
		case 'd':		opt_dirs = 1;			break;
		case 'E':		opt_extended_attributes = 1;	break;
		case 'e':		opt_rsh = optarg;		break;
		case 'f':
			push_filter(&opt_filter, optarg, FPT_INCL);	break;
		case 'g':		opt_group = 1;			break;
		case 'H':		opt_hard_links = 1;		break;
		case 'h':		opt_human_readable = 1;		break;
		case 'I':		opt_ignore_times = 1;		break;
		case 'i':		opt_itemize_changes = 1;	break;
		case 'K':		opt_keep_dirlinks = 1;		break;
		case 'k':		opt_copy_dirlinks = 1;		break;
		case 'L':		opt_copy_links = 1;		break;
		case 'l':		opt_links = 1;			break;
		case 'm':		opt_prune_empty_dirs = 1;	break;
		case 'N':
			if (!parsenum(&opt_streams, optarg, 0, 64))
				err(1, "streams: %s", optarg);
			break;
		case 'n':		opt_dry_run = 1;		break;
		case 'O':		opt_omit_dir_times = 1;		break;
		case 'o':		opt_owner = 1;			break;
		case 'P':		opt_progress = 1;
					opt_partial = 1;		break;
		case 'p':		opt_perms = 1;			break;
		case 'q':		opt_quiet = 1;			break;
		case 'R':		opt_relative = 1;		break;
		case 'r':		opt_recursive = 1;		break;
		case 'S':		opt_sparse = 1;			break;
		case 'T':		opt_temp_dir = optarg;		break;
		case 't':		opt_times = 1;			break;
		case 'u':		opt_update = 1;			break;
		case 'V':
			fprintf(stderr, "psync version %s\n", PSYNC_VERSION);
			exit(0);
			break;
		case 'v':		opt_verbose = 1;		break;
		case 'W':		opt_whole_file = 1;		break;
		case 'x':		opt_one_file_system = 1;	break;
		case 'y':		opt_fuzzy = 1;			break;
		case 'z':		opt_compress = 1;		break;
		case OPT_ADDRESS:	opt_address = optarg;		break;
		case OPT_BWLIMIT:
			if (!parsesize(&opt_bwlimit, optarg, 1024))
				err(1, "--bwlimit=%s", optarg);
			break;
		case OPT_CHMOD:		opt_chmod = optarg;		break;
		case OPT_COMPARE_DEST:	opt_compare_dest = optarg;	break;
		case OPT_COMPRESS_LEVEL:
			if (!parsenum(&opt_compress_level, optarg, 0, 10))
				err(1, "--compress-level=%s", optarg);
			break;
		case OPT_COPY_DEST:	opt_copy_dest = optarg;		break;
		case OPT_EXCLUDE:
			push_filter(&opt_filter, optarg, FPT_EXCL);	break;
		case OPT_EXCLUDE_FROM:
			pushfile(&opt_filter, optarg, push_filter,
			    FPT_EXCL);					break;
		case OPT_FILES_FROM:
			pushfile(&opt_files, optarg, push_files_from,
			    FPT_INCL);					break;
		case OPT_INCLUDE:
			push_filter(&opt_filter, optarg, FPT_INCL);	break;
		case OPT_INCLUDE_FROM:
			pushfile(&opt_filter, optarg, push_filter,
			    FPT_INCL);					break;
		case OPT_LINK_DEST:	opt_link_dest = optarg;		break;
		case OPT_LOG_FILE:	opt_log_file = optarg;		break;
		case OPT_LOG_FILE_FORMAT:
					opt_log_file_format = optarg;	break;
		case OPT_MAX_DELETE:
			if (!parsenum(&opt_max_delete, optarg, 0, INT_MAX))
				err(1, "--max-delete=%s", optarg);
			break;
		case OPT_MAX_SIZE:
			if (!parsesize(&opt_max_size, optarg, 1))
				err(1, "--max-size=%s", optarg);
			break;
		case OPT_MIN_SIZE:
			if (!parsesize(&opt_min_size, optarg, 1))
				err(1, "--min-size=%s", optarg);
			break;
		case OPT_MODIFY_WINDOW:
			if (!parsenum(&opt_modify_window, optarg, 0, INT_MAX))
				err(1, "--modify-window=%s", optarg);
			break;
		case OPT_ONLY_WRITE_BATCH:opt_write_batch = optarg;	break;
		case OPT_OUT_FORMAT:	opt_out_format = optarg;	break;
		case OPT_PORT:
			if (!parsenum(&opt_port, optarg, 0, 65535))
				err(1, "--port=%s", optarg);
			break;
		case OPT_PARTIAL_DIR:	opt_partial_dir = optarg;	break;
		case OPT_PASSWORD_FILE:	opt_password_file = optarg;	break;
		case OPT_PSYNC_PATH:	opt_psync_path = optarg;	break;
		case OPT_READ_BATCH:	opt_read_batch = optarg;	break;
		case OPT_SOCKOPTS:	opt_sockopts = optarg;		break;
		case OPT_SUFFIX:	opt_suffix = optarg;		break;
		case OPT_TIMEOUT:
			if (!parsenum(&opt_timeout, optarg, 0, INT_MAX))
				err(1, "--timeout=%s", optarg);
			break;
		case OPT_WRITE_BATCH:	opt_write_batch = optarg;	break;
		case 0:
			break;
		default:
			warn("invalid option: -%c", c);
			usage();
		}
	}
	argc -= optind;
	argv += optind;

	pscthr_init(THRT_MAIN, 0, NULL, NULL, 0, "main");

	psc_poolmaster_init(&buf_poolmaster, struct buf, lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "buf");
	buf_pool = psc_poolmaster_getmgr(&buf_poolmaster);

	psc_poolmaster_init(&work_poolmaster, struct work, wk_lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "work");
	work_pool = psc_poolmaster_getmgr(&work_poolmaster);

	fcache_init();

	lc_reginit(&workq, struct work, wk_lentry, "workq");

	pthread_barrier_init(&work_barrier, NULL, opt_streams);

	if (opt_puppet)
		exit(puppet_mode());

	psync_is_master = 1;

	if (argc < 2 ||
	    (argc == 1 && psc_dynarray_len(&opt_files) == 0))
		usage();

	/*
	 * psync a ... b
	 * psync nonexist ... b
	 * psync loc rem:fn
	 * psync nonexist ... rem:fn
	 * psync rem:fn ... loc
	 *
	 * psync rem1:fn ... rem2:fn2
	 */
	p = argv[--argc];
	dstfn = strchr(p, ':');
	if (dstfn) {
		*dstfn++ = '\0';
		host = p;
		mode = MODE_PUT;
	} else {
		struct stat stb;

		/* psync remote:file [dir/]file */
		/* psync remote:file dir */
		if (stat(p, &stb) == 0 && S_ISDIR(stb.st_mode)) {
			dstfn = "";
		} else {
			dstfn = strrchr(p, '/');
			if (dstfn > p)
				*dstfn++ = '\0';
			else
				p = NULL;
		}
		if (p && chdir(p) == -1)
			errx(1, "%s", p);

		host = argv[0];
		for (i = 0; i < argc; i++) {
			p = strchr(argv[i], ':');
			if (p == NULL)
				errx(1, "no source host specified");
			*p++ = '\0';
			if (strcmp(argv[i], host)) {
				errno = ENOTSUP;
				errx(1, "multiple source hosts");
			}
			argv[i] = p;
		}
		mode = MODE_GET;
	}

	psc_iostats_init(&iostats, "iostats");
	psc_tiosthr_spawn(THRT_TIOS, "tios");

	for (i = 0; i < opt_streams; i++) {
		struct recvthr *rt;
		struct stream *st;

		thr = pscthr_init(THRT_WK, 0, wkthr_main, NULL, 0,
		    "wkthr%d", i);
		push(&threads, thr);

		/*
		 * add:
		 *	-r
		 *	--sparse
		 *	--exclude filter patterns
		 */
		st = stream_cmdopen("%s %s %s --PUPPET",
		    opt_rsh, host, opt_psync_path);

		psc_dynarray_add(&streams, st);

		thr = pscthr_init(THRT_RECV, 0, recvthr_main, NULL,
		    sizeof(*rt), "recvthr%d", i);
		rt = thr->pscthr_private;
		rt->st = st;
		pscthr_setready(thr);
		push(&threads, thr);
	}

	flags = PFL_FILEWALKF_RELPATH;
	if (opt_recursive)
		flags |= PFL_FILEWALKF_RECURSIVE;
	if (opt_verbose)
		flags |= PFL_FILEWALKF_VERBOSE;

	signal(SIGINT, handle_signal);
	signal(SIGPIPE, handle_signal);

	pscthr_init(THRT_DISP, 0, dispthr_main, NULL, 0, "dispthr");

	for (i = 0; i < argc; i++) {
		rv = walkfiles(mode, argv[i], flags, dstfn);
		if (rv)
			rc = rv;
	}
	DYNARRAY_FOREACH(fn, i, &opt_files) {
		rv = filesfrom(mode, fn, flags, dstfn);
		if (rv)
			rc = rv;
	}
	lc_kill(&workq);

	DYNARRAY_FOREACH(thr, i, &threads) {
		rv = pthread_join(thr->pscthr_pthread, NULL);
		if (rv)
			rc = rv;
	}
	fcache_destroy();

	exit(rc);
}
