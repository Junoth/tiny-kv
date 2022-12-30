package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	value, _ := reader.GetCF(req.GetCf(), req.GetKey())
	res := kvrpcpb.RawGetResponse{}
	if value == nil {
		res.NotFound = true
	} else {
		res.Value = value
	}

	return &res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf(),
	}
	modify := storage.Modify{Data: put}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	res := kvrpcpb.RawPutResponse{}
	return &res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	put := storage.Delete{
		Key: req.GetKey(), Cf: req.GetCf(),
	}
	modify := storage.Modify{Data: put}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	res := kvrpcpb.RawDeleteResponse{}
	return &res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}

	res := kvrpcpb.RawScanResponse{}
	iter := reader.IterCF(req.GetCf())
	num := uint32(0)
	iter.Seek(req.GetStartKey())
	for iter.Valid() && num < req.GetLimit() {
		item := iter.Item()
		kvpair := kvrpcpb.KvPair{}
		kvpair.Key = item.Key()
		kvpair.Value, err = item.Value()
		if err != nil {
			log.Fatal("Failed to get value for key %v", kvpair.Key)
		} else {
			res.Kvs = append(res.Kvs, &kvpair)
			num += 1
		}

		iter.Next()
	}

	return &res, nil
}
