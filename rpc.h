/* $Id$ */
/* %PSC_COPYRIGHT% */

#ifndef _RPC_H_
#define _RPC_H_

struct hdr {
	uint32_t		opc;	/* operation code */
	uint32_t		msglen;	/* length of payload */
	uint64_t		xid;	/* message ID */
};

#define OPC_GETFILE		0
#define OPC_PUTDATA		1
#define OPC_EXAMINE		2
#define OPC_CHECKZERO		2
#define OPC_MAPNAME		3
#define OPC_CTL			4

struct rpc_sub_stat {
	uint64_t		dev;
	uint64_t		rdev;
	uint32_t		mode;
	uint32_t		uid;
	uint32_t		gid;
	uint32_t		_pad;
	uint64_t		size;
	struct pfl_timespec	atim;	/* access time */
	struct pfl_timespec	mtim;	/* modify (data) time */
	struct pfl_timespec	ctim;	/* change (metadata) time */
	struct pfl_timespec	btim;	/* birth time */
};

struct rpc_generic_rep {
	uint64_t		xid;
	int32_t			rc;
	int32_t			_pad;
};

struct rpc_getfile_req {
	uint32_t		flags;		/* see RPC_GETFILE_F_* */
	uint32_t		_pad;
	char			fn[0];
};

/* rpc_getfile_req flags */
#define RPC_GETFILE_F_RECURSE	(1 << 0)

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

struct rpc_getcrc_req {
	uint64_t		off;
	uint64_t		len;
};

#define ALGLEN 32
struct rpc_getcrc_rep {
	uint64_t		digest[ALGLEN];
};

struct rpc_pushname {
	struct rpc_sub_stat	pstb;
	char			fn[0];
};

struct rpc_ctl {
};

#endif /* _RPC_H_ */