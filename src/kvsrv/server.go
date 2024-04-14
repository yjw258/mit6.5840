package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVPairs   map[string]string
	PutAppendIDs map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.KVPairs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.PutAppendIDs[args.ID]
	if ok {
		return
	}
	delete(kv.PutAppendIDs, args.LastOkOperationID)
	kv.PutAppendIDs[args.ID] = ""
	kv.KVPairs[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	t, ok := kv.PutAppendIDs[args.ID]
	if ok {
		reply.Value = t
		return
	}
	delete(kv.PutAppendIDs, args.LastOkOperationID)
	oldValue := kv.KVPairs[args.Key]
	kv.PutAppendIDs[args.ID] = oldValue
	kv.KVPairs[args.Key] += args.Value
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.KVPairs = make(map[string]string)
	kv.PutAppendIDs = make(map[int64]string)
	return kv
}
