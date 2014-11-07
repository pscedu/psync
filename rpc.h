/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _RPC_H_
#define _RPC_H_

struct hdr {
	uint32_t		opc;	/* operation code */
	uint32_t		msglen;	/* length of payload */
	uint64_t		xid;	/* message ID */
};

#define OPC_GETFILE_REQ		0
#define OPC_GETFILE_REP		1
#define OPC_PUTDATA		2
#define OPC_CHECKZERO_REQ	3
#define OPC_CHECKZERO_REP	4
#define OPC_GETCKSUM_REQ	5
#define OPC_GETCKSUM_REP	6
#define OPC_PUTNAME_REQ		7
#define OPC_PUTNAME_REP		8
#define OPC_DONE		9

struct rpc_sub_stat {
	uint64_t		dev;
	uint64_t		rdev;
	uint32_t		mode;
	uint32_t		uid;
	uint32_t		gid;
	uint32_t		_pad;
	uint64_t		size;
	struct pfl_timespec	tim[2];
#define atim tim[0]			/* access time */
#define mtim tim[1]			/* modify (data) time */
};

struct rpc_generic_rep {
	uint64_t		xid;
	 int32_t		rc;
	 int32_t		_pad;
};

struct rpc_getfile_req {
	 int32_t		len;
	 int32_t		_pad;
	char			fn[0];	/* relative path */
//	char			base[0];/* destination basename (optional) */
};

#define rpc_getfile_rep rpc_generic_rep

struct rpc_putdata {
	uint64_t		fid;
	uint64_t		off;
	unsigned char		data[0];
};

struct rpc_checkzero_req {
	uint64_t		fid;
	uint64_t		off;
	uint64_t		len;
};

#define rpc_checkzero_rep rpc_generic_rep

struct rpc_getcksum_req {
	uint64_t		fid;
	uint64_t		off;
	uint64_t		len;
};

#define ALGLEN 32
struct rpc_getcksum_rep {
	char			digest[ALGLEN];
};

struct rpc_putname_req {
	struct rpc_sub_stat	pstb;
	uint64_t		fid;
	 int32_t		flags;
	 int32_t		_pad;
	char			fn[0];
};

struct rpc_putname_rep {
	uint64_t		fid;
	int32_t			rc;
};

#define RPC_PUTNAME_F_TRYDIR	(1 << 0)	/* try directory as base */

void rpc_send_done(struct stream *);
void rpc_send_getfile(struct stream *, uint64_t, const char *,
	const char *);
void rpc_send_putdata(struct stream *, uint64_t, off_t, const void *,
	size_t);
void rpc_send_putname_req(struct stream *, uint64_t, const char *,
	const struct stat *, const char *, int);
void rpc_send_putname_rep(struct stream *, uint64_t, int);

void handle_signal(int);

#endif /* _RPC_H_ */
