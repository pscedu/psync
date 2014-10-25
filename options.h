/* $Id$ */
/* %PSC_COPYRIGHT% */

#ifndef _OPTIONS_H_
#define _OPTIONS_H_

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
	OPT_WRITE_BATCH,

	/* psync specific options */
	OPT_DSTDIR,
	OPT_HEAD,
	OPT_PUPPET
};

struct options {
	const char		*address;
	uint64_t		 block_size;
	const char		*chmod;
	const char		*compare_dest;
	const char		*copy_dest;
	const char		*link_dest;
	const char		*log_file;
	const char		*log_file_format;
	int			 max_delete;
	uint64_t		 max_size;
	uint64_t		 min_size;
	int			 modify_window;
	const char		*out_format;
	const char		*partial_dir;
	const char		*password_file;
	const char		*psync_path;
	const char		*read_batch;
	const char		*rsh;
	const char		*sockopts;
	const char		*suffix;
	const char		*temp_dir;
	const char		*write_batch;
	int			 _8_bit_output;
	int			 append;
	int			 backup;
	int			 blocking_io;
	uint64_t		 bwlimit;
	int			 cache;
	int			 checksum;
	int			 compress;
	int			 compress_level;
	int			 copy_dirlinks;
	int			 copy_links;
	int			 copy_unsafe_links;
	int			 cvs_exclude;
	int			 del;
	int			 delay_updates;
	int			 delete_;
	int			 delete_after;
	int			 delete_before;
	int			 delete_during;
	int			 delete_excluded;
	int			 devices;
	int			 dirs;
	int			 dry_run;
	struct psc_dynarray	 exclude;
	struct psc_dynarray	 files;
	struct psc_dynarray	 filter;
	struct psc_dynarray	 include;
	int			 executability;
	int			 existing;
	int			 extended_attributes;
	int			 force;
	int			 from0;
	int			 fuzzy;
	int			 group;
	int			 hard_links;
	int			 human_readable;
	int			 ignore_errors;
	int			 ignore_existing;
	int			 ignore_times;
	int			 inplace;
	int			 ipv4;
	int			 ipv6;
	int			 itemize_changes;
	int			 keep_dirlinks;
	int			 links;
	int			 list_only;
	int			 no_implied_dirs;
	int			 numeric_ids;
	int			 omit_dir_times;
	int			 one_file_system;
	int			 owner;
	int			 partial;
	int			 perms;
	int			 port;
	int			 progress;
	int			 prune_empty_dirs;
	int			 quiet;
	int			 recursive;
	int			 relative;
	int			 remove_source_file;
	int			 remove_source_files;
	int			 safe_links;
	int			 size_only;
	int			 sparse;
	int			 specials;
	int			 stats;
	int			 super;
	int			 timeout;		/* in seconds */
	int			 times;
	int			 update;
	int			 verbose;
	int			 whole_file;

	/* psync specific options */
	int			 puppet;
	int			 streams;
	int			 head;
	const char		*dstdir;
};

void parseopts(int, char **);

extern struct options opts;

#endif /* _OPTIONS_H_ */
