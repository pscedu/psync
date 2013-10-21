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
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "pfl/types.h"
#include "pfl/walk.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/alloc.h"
#include "pfl/crc.h"
#include "pfl/log.h"
#include "pfl/pool.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"

enum {
	THRT_MAIN,
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
const char		*opt_psync_path;
const char		*opt_read_batch;
const char		*opt_rsh = "ssh";
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
int			 opt_nthreads;
int			 opt_numeric_ids;
int			 opt_omit_dir_times;
int			 opt_one_file_system;
int			 opt_owner;
int			 opt_partial;
int			 opt_perms;
int			 opt_port;
int			 opt_progress;
int			 opt_prune_empty_dirs;
int			 opt_quiet;
int			 opt_recursive;
int			 opt_relative;
int			 opt_remove_source_file;
int			 opt_remove_source_files;
int			 opt_safe_links;
int			 opt_sink;
int			 opt_size_only;
int			 opt_sparse;
int			 opt_specials;
int			 opt_stats;
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

struct psc_poolmaster	 work_poolmaster;
struct psc_poolmgr	*work_pool;

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
	{ "address",		REQARG,	NULL,			OPT_ADDRESS },
	{ "append",		NO_ARG,	&opt_append,		0x1 },
	{ "archive",		NO_ARG,	NULL,			'a' },
	{ "backup",		NO_ARG,	NULL,			'b' },
	{ "backup-dir",		NO_ARG,	NULL,			0x1 },
	{ "block-size",		REQARG,	NULL,			'B' },
	{ "blocking-io",	NO_ARG,	&opt_blocking_io,	0x1 },
	{ "bwlimit",		REQARG,	NULL,			OPT_BWLIMIT },
	{ "cache",		NO_ARG,	&opt_cache,		0x1 },
	{ "checksum",		NO_ARG,	NULL,			'c' },
	{ "chmod",		REQARG,	NULL,			OPT_CHMOD },
	{ "compare-dest",	REQARG,	NULL,			OPT_COMPARE_DEST },
	{ "compress",		NO_ARG,	NULL,			'z' },
	{ "compress-level",	REQARG,	NULL,			OPT_COMPRESS_LEVEL },
	{ "copy-dest",		REQARG,	NULL,			OPT_COPY_DEST },
	{ "copy-dirlinks",	NO_ARG,	NULL,			'k' },
	{ "copy-links",		NO_ARG,	NULL,			'L' },
	{ "copy-unsafe-links",	NO_ARG,	&opt_copy_unsafe_links,	0x1 },
	{ "cvs-exclude",	NO_ARG,	NULL,			'C' },
	{ "del",		NO_ARG,	&opt_del,		0x1 },
	{ "delay-updates",	NO_ARG,	&opt_delay_updates,	0x1 },
	{ "delete",		NO_ARG,	&opt_delete,		0x1 },
	{ "delete-after",	NO_ARG,	&opt_delete_after,	0x1 },
	{ "delete-before",	NO_ARG,	&opt_delete_before,	0x1 },
	{ "delete-during",	NO_ARG,	&opt_delete_during,	0x1 },
	{ "delete-excluded",	NO_ARG,	&opt_delete_excluded,	0x1 },
	{ "devices",		NO_ARG,	&opt_devices,		0x1 },
	{ "dirs",		NO_ARG,	NULL,			'd' },
	{ "dry-run",		NO_ARG,	NULL,			'n' },
	{ "exclude",		REQARG,	NULL,			OPT_EXCLUDE },
	{ "exclude-from",	REQARG,	NULL,			OPT_EXCLUDE_FROM },
	{ "executability",	NO_ARG,	&opt_excutability,	0x1 },
	{ "existing",		NO_ARG,	&opt_existing,		0x1 },
	{ "extended-attributes",NO_ARG,	NULL,			'E' },
	{ "files-from",		REQARG,	NULL,			OPT_FILES_FROM },
	{ "filter",		REQARG,	NULL,			'f' },
	{ "force",		NO_ARG,	&opt_force,		0x1 },
	{ "from0",		NO_ARG,	NULL,			'0' },
	{ "fuzzy",		NO_ARG,	NULL,			'y' },
	{ "group",		NO_ARG,	NULL,			'g' },
	{ "hard-links",		NO_ARG,	NULL,			'H' },
	{ "human-readable",	NO_ARG,	NULL,			'h' },
	{ "ignore-errors",	NO_ARG,	&opt_ignore_errors,	0x1 },
	{ "ignore-existing",	NO_ARG,	&opt_ignore_existing,	0x1 },
	{ "ignore-times",	NO_ARG,	NULL,			'I' },
	{ "include",		REQARG,	NULL,			OPT_INCLUDE },
	{ "include-from",	REQARG,	NULL,			OPT_INCLUDE_FROM },
	{ "inplace",		NO_ARG,	&opt_inplace,		0x1 },
	{ "ipv4",		NO_ARG,	NULL,			'4' },
	{ "ipv6",		NO_ARG,	NULL,			'6' },
	{ "itemize-changes",	NO_ARG,	NULL,			'i' },
	{ "keep-dirlinks",	NO_ARG,	NULL,			'K' },
	{ "link-dest",		REQARG,	NULL,			OPT_LINK_DEST },
	{ "links",		NO_ARG,	NULL,			'l' },
	{ "list-only",		NO_ARG,	&opt_list_only,		0x1 },
	{ "log-file",		REQARG,	NULL,			OPT_LOG_FILE },
	{ "log-file-format",	REQARG,	NULL,			OPT_LOG_FILE_FORMAT },
	{ "max-delete",		REQARG,	NULL,			OPT_MAX_DELETE },
	{ "max-size",		REQARG,	NULL,			OPT_MAX_SIZE },
	{ "min-size",		REQARG,	NULL,			OPT_MIN_SIZE },
	{ "modify-window",	REQARG,	NULL,			OPT_MODIFY_WINDOW },
	{ "no-implied-dirs",	NO_ARG,	&opt_no_implied_dirs,	0x1 },
	{ "numeric-ids",	NO_ARG,	&opt_numeric_ids,	0x1 },
	{ "omit-dir-times",	NO_ARG,	NULL,			'O' },
	{ "one-file-system",	NO_ARG,	NULL,			'x' },
	{ "only-write-batch",	REQARG,	NULL,			OPT_ONLY_WRITE_BATCH },
	{ "out-format",		REQARG,	NULL,			OPT_OUT_FORMAT },
	{ "owner",		NO_ARG,	NULL,			'o' },
	{ "partial",		NO_ARG,	&opt_partial,		0x1 },
	{ "partial-dir",	REQARG,	NULL,			OPT_PARTIAL_DIR },
	{ "password-file",	REQARG,	NULL,			OPT_PASSWORD_FILE },
	{ "perms",		NO_ARG,	NULL,			'p' },
	{ "port",		REQARG,	NULL,			OPT_PORT },
	{ "progress",		NO_ARG,	&opt_progress,		0x1 },
	{ "prune-empty-dirs",	NO_ARG,	NULL,			'm' },
	{ "psync-path",		REQARG,	NULL,			OPT_PSYNC_PATH },
	{ "quiet",		NO_ARG,	NULL,			'q' },
	{ "read-batch",		REQARG,	NULL,			OPT_READ_BATCH },
	{ "recursive",		NO_ARG,	NULL,			'r' },
	{ "relative",		NO_ARG,	NULL,			'R' },
	{ "remove-source-files",NO_ARG,	&opt_remove_source_files,0x1 },
	{ "rsh",		REQARG,	NULL,			'e' },
	{ "safe-links",		NO_ARG,	&opt_safe_links,	0x1 },
	{ "SINK",		NO_ARG,	&opt_sink,		0x1 },
	{ "size-only",		NO_ARG,	&opt_size_only,		0x1 },
	{ "sockopts",		REQARG,	NULL,			OPT_SOCKOPTS },
	{ "sparse",		NO_ARG,	NULL,			'S' },
	{ "specials",		NO_ARG,	&opt_specials,		0x1 },
	{ "stats",		NO_ARG,	&opt_stats,		0x1 },
	{ "suffix",		REQARG,	NULL,			OPT_SUFFIX },
	{ "super",		NO_ARG,	&opt_super,		0x1 },
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

struct work {
	struct psc_listentry	  wk_lentry;
	char			  wk_fn[PATH_MAX];
	struct stat		  wk_stb;
	void			(*wk_cb)(struct work *);
};

struct psc_listcache workq;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-Pqvx]\n", progname);
	exit(1);
}

void
worker_main(struct psc_thread *thr)
{
	struct work *wk;

	while (pscthr_run()) {
		wk = lc_getwait(&workq);
		if (wk == NULL)
			break;
		wk->wk_cb(wk);
		psc_pool_return(work_pool, wk);
	}
}

int
parsenum(int *p, const char *s, int min, int max)
{
	char *endp;
	long l;

	l = strtol(s, &endp, 0);
	if (l < min || l > max) {
		errno = ERANGE;
		return (0);
	}
	if (endp == s || *endp) {
		errno = EINVAL;
		return (0);
	}
	*p = l;
	return (1);
}

int
parsesize(uint64_t *p, const char *s, uint64_t base)
{
	const char *bases = "bkmgtpe";
	char *endp, *b;
	uint64_t l;

	l = strtoull(s, &endp, 0);
	if (endp == s) {
		errno = EINVAL;
		return (0);
	}
	if (*endp) {
		b = strchr(bases, *endp);
		if (b == NULL || endp[1]) {
			errno = EINVAL;
			return (0);
		}
		base = UINT64_C(1) << (10 * (b - bases));
	}
	*p = l * base;
	return (1);
}

int
walkfile(const char *fn, const struct stat *stb, void *arg)
{
	struct work *wk;
	int j, rc = 0;

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

	wk = psc_pool_get(work_pool);
//	wk->wk_cb = i;
	strlcpy(wk->wk_srcfn, fn, sizeof(wk->wk_srcfn));
	strlcpy(wk->wk_dstfn, fn, sizeof(wk->wk_dstfn));
	memcpy(&wk->wk_stb, stb, sizeof(wk->wk_stb));
	lc_add(&workq, wk);
	return (rc);
}

#define push(da, ent)							\
	do {								\
		if (psc_dynarray_add((da), (ent)))			\
			err(1, NULL);					\
	} while (0)

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
	psc_dynarray_add(da, fn);
}

int
main(int argc, char *argv[])
{
	int flags, i, rv, rc = 0, c;
	struct psc_dynarray threads = DYNARRAY_INIT;
	struct psc_thread *thr;
	char *fn;

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
			if (!parsenum(&opt_nthreads, optarg, 0, 100))
				err(1, "-n %s", optarg);
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
			fprintf(stderr, "psync version %d\n", VERSION);
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
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (argc < 2 ||
	    (argc == 1 && psc_dynarray_len(&opt_files) == 0))
		usage();

	pscthr_init(THRT_MAIN, 0, NULL, NULL, 0, "main");
	psc_tiosthr_spawn(THRT_TIOS, "tios");

	lc_reginit(&workq, struct work, wk_lentry, "workq");

	fn = strchr(argv[0], ':');
	if (opt_sink || fn) {

	} else {
		argc--;
		fn = strchr(argv[argc], ':');
		if (fn == NULL)
			usage();

		for (i = 0; i < opt_nthreads; i++) {
			thr = pscthr_init(THRT_WK, 0, worker_main, NULL,
			    0, "wk%d", i);
			psc_dynarray_add(&threads, thr);
		}
		flags = 0;
		if (opt_recursive)
			flags |= PFL_FILEWALKF_RECURSIVE;
		if (opt_verbose)
			flags |= PFL_FILEWALKF_VERBOSE;
		for (i = 0; i < argc; i++) {
			rv = pfl_filewalk(argv[i], flags, walkfile,
			    argv[argc]);
			if (rv)
				rc = rv;
		}
		DYNARRAY_FOREACH(fn, i, &opt_files) {
			rv = pfl_filewalk(fn, flags, walkfile,
			    argv[argc]);
			if (rv)
				rc = rv;
		}
		lc_kill(&workq);
		DYNARRAY_FOREACH(thr, i, &threads) {
			rv = pthread_join(thr->pscthr_pthread, NULL);
			if (rv)
				rc = rv;
		}
	}
	exit(rc);
}
