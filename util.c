/* $Id$ */
/* %PSC_COPYRIGHT% */

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/dynarray.h"

char **
str_split(char *s)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	char **v, *p, *beg;
	int delim, esc;
	size_t len;

	for (p = beg = s; *p; ) {
		if (isspace(*p)) {
			*p++ = '\0';
			while (isspace(*p))
				p++;
			beg = p;
			continue;
		}
		if (*p == '\'' ||
		    *p == '"') {
			delim = *p;
			esc = 0;

			for (; *p; p++) {
				if (esc) {
					esc = 0;
					continue;
				}
				if (*p == delim)
					break;
				else if (*p == '\\')
					esc = 1;
			}
		}
		if (beg == p)
			push(&a, beg);
		p++;
	}

	push(&a, NULL);

	len = sizeof(*v) * psc_dynarray_len(&a);
	v = PSCALLOC(len + sizeof(*v));
	memcpy(v, psc_dynarray_get(&a), len);
	return (v);
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
