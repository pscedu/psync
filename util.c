/* $Id$ */

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