/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <ctype.h>
#include <getopt.h>
#include <stdint.h>

#include "pfl/alloc.h"
#include "pfl/dynarray.h"
#include "pfl/str.h"

#include "psync.h"
#include "options.h"

struct options		 opts;

#define NO_ARG		no_argument
#define REQARG		required_argument

struct option longopts[] = {
	{ "8-bit-output",	NO_ARG,	NULL,			'8' },
	{ "HEAD",		NO_ARG,	&opts.head,		OPT_HEAD },
	{ "PUPPET",		REQARG,	NULL,			OPT_PUPPET },
	{ "address",		REQARG,	NULL,			OPT_ADDRESS },
	{ "append",		NO_ARG,	&opts.append,		1 },
	{ "archive",		NO_ARG,	NULL,			'a' },
	{ "backup",		NO_ARG,	NULL,			'b' },
	{ "backup-dir",		NO_ARG,	NULL,			1 },
	{ "block-size",		REQARG,	NULL,			'B' },
	{ "blocking-io",	NO_ARG,	&opts.blocking_io,	1 },
	{ "bwlimit",		REQARG,	NULL,			OPT_BWLIMIT },
	{ "cache",		NO_ARG,	&opts.cache,		1 },
	{ "checksum",		NO_ARG,	NULL,			'c' },
	{ "chmod",		REQARG,	NULL,			OPT_CHMOD },
	{ "compare-dest",	REQARG,	NULL,			OPT_COMPARE_DEST },
	{ "compress",		NO_ARG,	NULL,			'z' },
	{ "compress-level",	REQARG,	NULL,			OPT_COMPRESS_LEVEL },
	{ "copy-dest",		REQARG,	NULL,			OPT_COPY_DEST },
	{ "copy-dirlinks",	NO_ARG,	NULL,			'k' },
	{ "copy-links",		NO_ARG,	NULL,			'L' },
	{ "copy-unsafe-links",	NO_ARG,	&opts.copy_unsafe_links,1 },
	{ "cvs-exclude",	NO_ARG,	NULL,			'C' },
	{ "del",		NO_ARG,	&opts.del,		1 },
	{ "delay-updates",	NO_ARG,	&opts.delay_updates,	1 },
	{ "delete",		NO_ARG,	&opts.delete_,		1 },
	{ "delete-after",	NO_ARG,	&opts.delete_after,	1 },
	{ "delete-before",	NO_ARG,	&opts.delete_before,	1 },
	{ "delete-during",	NO_ARG,	&opts.delete_during,	1 },
	{ "delete-excluded",	NO_ARG,	&opts.delete_excluded,	1 },
	{ "devices",		NO_ARG,	&opts.devices,		1 },
	{ "dirs",		NO_ARG,	NULL,			'd' },
	{ "dry-run",		NO_ARG,	NULL,			'n' },
	{ "exclude",		REQARG,	NULL,			OPT_EXCLUDE },
	{ "exclude-from",	REQARG,	NULL,			OPT_EXCLUDE_FROM },
	{ "executability",	NO_ARG,	&opts.executability,	1 },
	{ "existing",		NO_ARG,	&opts.existing,		1 },
	{ "extended-attributes",NO_ARG,	NULL,			'E' },
	{ "files-from",		REQARG,	NULL,			OPT_FILES_FROM },
	{ "filter",		REQARG,	NULL,			'f' },
	{ "force",		NO_ARG,	&opts.force,		1 },
	{ "from0",		NO_ARG,	NULL,			'0' },
	{ "fuzzy",		NO_ARG,	NULL,			'y' },
	{ "group",		NO_ARG,	NULL,			'g' },
	{ "hard-links",		NO_ARG,	NULL,			'H' },
	{ "human-readable",	NO_ARG,	NULL,			'h' },
	{ "ignore-errors",	NO_ARG,	&opts.ignore_errors,	1 },
	{ "ignore-existing",	NO_ARG,	&opts.ignore_existing,	1 },
	{ "ignore-times",	NO_ARG,	NULL,			'I' },
	{ "include",		REQARG,	NULL,			OPT_INCLUDE },
	{ "include-from",	REQARG,	NULL,			OPT_INCLUDE_FROM },
	{ "inplace",		NO_ARG,	&opts.inplace,		1 },
	{ "ipv4",		NO_ARG,	NULL,			'4' },
	{ "ipv6",		NO_ARG,	NULL,			'6' },
	{ "itemize-changes",	NO_ARG,	NULL,			'i' },
	{ "keep-dirlinks",	NO_ARG,	NULL,			'K' },
	{ "link-dest",		REQARG,	NULL,			OPT_LINK_DEST },
	{ "links",		NO_ARG,	NULL,			'l' },
	{ "list-only",		NO_ARG,	&opts.list_only,	1 },
	{ "log-file",		REQARG,	NULL,			OPT_LOG_FILE },
	{ "log-file-format",	REQARG,	NULL,			OPT_LOG_FILE_FORMAT },
	{ "max-delete",		REQARG,	NULL,			OPT_MAX_DELETE },
	{ "max-size",		REQARG,	NULL,			OPT_MAX_SIZE },
	{ "min-size",		REQARG,	NULL,			OPT_MIN_SIZE },
	{ "modify-window",	REQARG,	NULL,			OPT_MODIFY_WINDOW },
	{ "no-implied-dirs",	NO_ARG,	&opts.no_implied_dirs,	1 },
	{ "numeric-ids",	NO_ARG,	&opts.numeric_ids,	1 },
	{ "omit-dir-times",	NO_ARG,	NULL,			'O' },
	{ "one-file-system",	NO_ARG,	NULL,			'x' },
	{ "only-write-batch",	REQARG,	NULL,			OPT_ONLY_WRITE_BATCH },
	{ "out-format",		REQARG,	NULL,			OPT_OUT_FORMAT },
	{ "owner",		NO_ARG,	NULL,			'o' },
	{ "partial",		NO_ARG,	&opts.partial,		1 },
	{ "partial-dir",	REQARG,	NULL,			OPT_PARTIAL_DIR },
	{ "password-file",	REQARG,	NULL,			OPT_PASSWORD_FILE },
	{ "perms",		NO_ARG,	NULL,			'p' },
	{ "port",		REQARG,	NULL,			OPT_PORT },
	{ "progress",		NO_ARG,	&opts.progress,		1 },
	{ "prune-empty-dirs",	NO_ARG,	NULL,			'm' },
	{ "psync-path",		REQARG,	NULL,			OPT_PSYNC_PATH },
	{ "quiet",		NO_ARG,	NULL,			'q' },
	{ "read-batch",		REQARG,	NULL,			OPT_READ_BATCH },
	{ "recursive",		NO_ARG,	NULL,			'r' },
	{ "relative",		NO_ARG,	NULL,			'R' },
	{ "remove-source-files",NO_ARG,	&opts.remove_source_files,1 },
	{ "rsh",		REQARG,	NULL,			'e' },
	{ "safe-links",		NO_ARG,	&opts.safe_links,	1 },
	{ "size-only",		NO_ARG,	&opts.size_only,		1 },
	{ "sockopts",		REQARG,	NULL,			OPT_SOCKOPTS },
	{ "sparse",		NO_ARG,	NULL,			'S' },
	{ "specials",		NO_ARG,	&opts.specials,		1 },
	{ "stats",		NO_ARG,	&opts.stats,		1 },
	{ "suffix",		REQARG,	NULL,			OPT_SUFFIX },
	{ "super",		NO_ARG,	&opts.super,		1 },
	{ "temp-dir",		REQARG,	NULL,			'T' },
	{ "timeout",		REQARG,	NULL,			OPT_TIMEOUT },
	{ "times",		NO_ARG,	NULL,			't' },
	{ "update",		NO_ARG,	NULL,			'u' },
	{ "verbose",		NO_ARG,	NULL,			'v' },
	{ "version",		NO_ARG,	NULL,			'V' },
	{ "whole-file",		NO_ARG,	NULL,			'W' },
	{ "write-batch",	REQARG,	NULL,			OPT_WRITE_BATCH },

	/* psync specific options */
	{ "dstdir",		REQARG,	NULL,			OPT_DSTDIR },
	{ "streams",		REQARG,	&opts.streams,		'N' },

	{ NULL,			0,	NULL,			0 }
};

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
			psync_fatal("invalid format");
		sep = strchr(sty, ',');
		if (sep)
			*sep = '\0';

		for (n = 0, ty = types; n < nitems(types); ty++, n++)
			if (strcmp(ty->name, sty) == 0 ||
			    strcmp(ty->abbr, sty) == 0)
				break;
		if (n == nitems(types))
			psync_fatal("invalid format");
		fp->fp_type = ty->type;
		fp->fp_pat = s;
	}
	push(da, fp);
}

void
pushfile(struct psc_dynarray *da, char *fn,
    void (*f)(struct psc_dynarray *, char *, int), int arg)
{
	char *p, buf[BUFSIZ];
	FILE *fp;

	fp = fopen(fn, "r");
	if (fp == NULL)
		psync_fatal("%s", fn);
	while (fgets(buf, sizeof(buf), fp)) {
		p = pfl_strdup(buf);
		f(da, p, arg);
	}
	if (ferror(fp))
		psync_fatal("%s", fn);
	fclose(fp);
}

void
push_files_from(struct psc_dynarray *da, char *fn,
    __unusedx int arg)
{
	push(da, fn);
}

void
parseopts(int argc, char **argv)
{
	int c;

	/* initialize default option values */
	psc_dynarray_init(&opts.exclude);
	psc_dynarray_init(&opts.files);
	psc_dynarray_init(&opts.filter);
	psc_dynarray_init(&opts.include);
	opts.progress = 1;
	opts.psync_path = "psync";
	opts.rsh = "ssh -oControlPath=none -oKbdInteractiveAuthentication=no";
	opts.streams = getnstreams(MAX_STREAMS);

	while ((c = getopt_long(argc, argv,
	    "0468aB:bCcDdEEe:f:gHhIiKkLlmN:nOoPpqRrST:tuVvWxyz", longopts,
	    NULL)) != -1) {
		switch (c) {
		case '0':		opts.from0 = 1;			break;
		case '4':		opts.ipv4 = 1;			break;
		case '6':		opts.ipv6 = 1;			break;
		case '8':		opts._8_bit_output = 1;		break;
		case 'a':		opts.devices = 1;
					opts.group = 1;
					opts.links = 1;
					opts.owner = 1;
					opts.perms = 1;
					opts.recursive = 1;
					opts.specials = 1;
					opts.times = 1;			break;
		case 'B':
			if (!parsesize(&opts.block_size, optarg, 1))
				err(1, "-B %s", optarg);
			break;
		case 'b':		opts.backup = 1;		break;
		case 'C':		opts.cvs_exclude = 1;		break;
		case 'c':		opts.checksum = 1;		break;
		case 'D':		opts.devices = 1;
					opts.specials = 1;		break;
		case 'd':		opts.dirs = 1;			break;
		case 'E':		opts.extended_attributes = 1;	break;
		case 'e':		opts.rsh = optarg;		break;
		case 'f':
			push_filter(&opts.filter, optarg, FPT_INCL);	break;
		case 'g':		opts.group = 1;			break;
		case 'H':		opts.hard_links = 1;		break;
		case 'h':		opts.human_readable = 1;	break;
		case 'I':		opts.ignore_times = 1;		break;
		case 'i':		opts.itemize_changes = 1;	break;
		case 'K':		opts.keep_dirlinks = 1;		break;
		case 'k':		opts.copy_dirlinks = 1;		break;
		case 'L':		opts.copy_links = 1;		break;
		case 'l':		opts.links = 1;			break;
		case 'm':		opts.prune_empty_dirs = 1;	break;
		case 'N':
			if (!parsenum(&opts.streams, optarg, 0, MAX_STREAMS))
				err(1, "streams: %s", optarg);
			break;
		case 'n':		opts.dry_run = 1;		break;
		case 'O':		opts.omit_dir_times = 1;	break;
		case 'o':		opts.owner = 1;			break;
		case 'P':		opts.progress = 1;
					opts.partial = 1;		break;
		case 'p':		opts.perms = 1;			break;
		case 'q':		opts.quiet = 1;			break;
		case 'R':		opts.relative = 1;		break;
		case 'r':		opts.recursive = 1;		break;
		case 'S':		opts.sparse = 1;		break;
		case 'T':		opts.temp_dir = optarg;		break;
		case 't':		opts.times = 1;			break;
		case 'u':		opts.update = 1;		break;
		case 'V':
			fprintf(stderr, "psync version %d\n", PSYNC_VERSION);
			exit(0);
			break;
		case 'v':		opts.verbose = 1;		break;
		case 'W':		opts.whole_file = 1;		break;
		case 'x':		opts.one_file_system = 1;	break;
		case 'y':		opts.fuzzy = 1;			break;
		case 'z':		opts.compress = 1;		break;
		case OPT_ADDRESS:	opts.address = optarg;		break;
		case OPT_BWLIMIT:
			if (!parsesize(&opts.bwlimit, optarg, 1024))
				err(1, "--bwlimit=%s", optarg);
			break;
		case OPT_CHMOD:		opts.chmod = optarg;		break;
		case OPT_COMPARE_DEST:	opts.compare_dest = optarg;	break;
		case OPT_COMPRESS_LEVEL:
			if (!parsenum(&opts.compress_level, optarg, 0, 10))
				err(1, "--compress-level=%s", optarg);
			break;
		case OPT_COPY_DEST:	opts.copy_dest = optarg;	break;
		case OPT_EXCLUDE:
			push_filter(&opts.filter, optarg, FPT_EXCL);	break;
		case OPT_EXCLUDE_FROM:
			pushfile(&opts.filter, optarg, push_filter,
			    FPT_EXCL);					break;
		case OPT_FILES_FROM:
			pushfile(&opts.files, optarg, push_files_from,
			    FPT_INCL);					break;
		case OPT_INCLUDE:
			push_filter(&opts.filter, optarg, FPT_INCL);	break;
		case OPT_INCLUDE_FROM:
			pushfile(&opts.filter, optarg, push_filter,
			    FPT_INCL);					break;
		case OPT_LINK_DEST:	opts.link_dest = optarg;	break;
		case OPT_LOG_FILE:	opts.log_file = optarg;		break;
		case OPT_LOG_FILE_FORMAT:
					opts.log_file_format = optarg;	break;
		case OPT_MAX_DELETE:
			if (!parsenum(&opts.max_delete, optarg, 0, INT_MAX))
				err(1, "--max-delete=%s", optarg);
			break;
		case OPT_MAX_SIZE:
			if (!parsesize(&opts.max_size, optarg, 1))
				err(1, "--max-size=%s", optarg);
			break;
		case OPT_MIN_SIZE:
			if (!parsesize(&opts.min_size, optarg, 1))
				err(1, "--min-size=%s", optarg);
			break;
		case OPT_MODIFY_WINDOW:
			if (!parsenum(&opts.modify_window, optarg, 0, INT_MAX))
				err(1, "--modify-window=%s", optarg);
			break;
		case OPT_ONLY_WRITE_BATCH:opts.write_batch = optarg;	break;
		case OPT_OUT_FORMAT:	opts.out_format = optarg;	break;
		case OPT_PORT:
			if (!parsenum(&opts.port, optarg, 0, 65535))
				err(1, "--port=%s", optarg);
			break;
		case OPT_PARTIAL_DIR:	opts.partial_dir = optarg;	break;
		case OPT_PASSWORD_FILE:	opts.password_file = optarg;	break;
		case OPT_PSYNC_PATH:	opts.psync_path = optarg;	break;
		case OPT_READ_BATCH:	opts.read_batch = optarg;	break;
		case OPT_SOCKOPTS:	opts.sockopts = optarg;		break;
		case OPT_SUFFIX:	opts.suffix = optarg;		break;
		case OPT_TIMEOUT:
			if (!parsenum(&opts.timeout, optarg, 0, INT_MAX))
				err(1, "--timeout=%s", optarg);
			break;
		case OPT_WRITE_BATCH:	opts.write_batch = optarg;	break;

		/* psync specific options */
		case OPT_DSTDIR:	opts.dstdir = optarg;		break;
		case OPT_PUPPET:
			if (!parsenum(&opts.puppet, optarg, 0, 1000000))
				err(1, "--PUPPET=%s", optarg);
			break;

		case 0:
			break;
		default:
			warn("invalid option: -%c", c);
			usage();
		}
	}
}
