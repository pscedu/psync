/*
 * %ISC_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright (c) 2011-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/stat.h>

#include <ctype.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/dynarray.h"

#include "psync.h"

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

void
psync_chown(const char *fn, uid_t uid, gid_t gid, int flags)
{
//	static int mask;
//
 //retry:
//	if (mask)
//		flags &= ~mask;
	if (fchownat(AT_FDCWD, fn, uid, gid, flags) == -1) {
//		if (errno == ENOTSUP && flags & AT_SYMLINK_NOFOLLOW) {
//			mask = AT_SYMLINK_NOFOLLOW;
//			goto retry;
//		}
		psynclog_warn("chown %s", fn);
	}
}

void
psync_chmod(const char *fn, mode_t mode, int flags)
{
	static int notsup;

	if (notsup)
		return;
	if (fchmodat(AT_FDCWD, fn, mode, flags) == -1) {
		int rc = errno;

		if (rc == ENOTSUP && flags & AT_SYMLINK_NOFOLLOW)
			notsup = 1;
		else
			psynclog_warn("chmod %s", fn); 
	}
}

void
psync_utimes(const char *fn, const struct pfl_timespec *pts, int flags)
{
#ifdef HAVE_FUTIMENS
	struct timespec ts[2];

	ts[0].tv_sec = pts[0].tv_sec;
	ts[0].tv_nsec = pts[0].tv_nsec;

	ts[1].tv_sec = pts[1].tv_sec;
	ts[1].tv_nsec = pts[1].tv_nsec;

	if (utimensat(AT_FDCWD, fn, ts, flags) == -1)
		psynclog_warn("utimes %s", fn);
#else
	struct timeval tv[2];

	(void)flags;

	tv[0].tv_sec = pts[0].tv_sec;
	tv[0].tv_usec = pts[0].tv_nsec / 1000;

	tv[1].tv_sec = pts[1].tv_sec;
	tv[1].tv_usec = pts[1].tv_nsec / 1000;

	if (lutimes(fn, tv) == -1)
		psynclog_warn("utimes %s", fn);
#endif
}
