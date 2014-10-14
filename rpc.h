/* $Id$ */
/* %PSC_COPYRIGHT% */

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
#define OPC_PUTNAME		7
#define OPC_DONE		8

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
	char			fn[0];
	/*
	 * Followed by file basename, used if not already specified
	 * locally.
	 */
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

struct rpc_putname {
	struct rpc_sub_stat	pstb;
	uint64_t		fid;
	 int32_t		dirlen;
	 int32_t		_pad;
	char			dir[0];	/* might be file */
	/*
	 * Followed by file basename, used if not already specified
	 * locally.
	 */
};

struct rpc_done {
	 int32_t		clean;
	 int32_t		_pad;
};

void rpc_send_done(struct stream *, int);
void rpc_send_getfile(uint64_t, const char *);
void rpc_send_putdata(uint64_t, off_t, const void *, size_t);
void rpc_send_putname(const char *, const char *, const struct stat *);

void handle_signal(int);

#endif /* _RPC_H_ */
