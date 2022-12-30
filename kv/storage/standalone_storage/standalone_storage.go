package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf   *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	log.Debug("Created a new standalone storage")
	standAloneStorage := StandAloneStorage{conf, nil}
	return &standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Configure badger db
	if s == nil {
		return errors.New("standalone storage is nil")
	}
	kvPath := filepath.Join(s.conf.DBPath, "kv")
	raftPath := filepath.Join(s.conf.DBPath, "raft")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)
	s.engine = engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	log.Debug("Start the standalone storage")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s == nil {
		return errors.New("standalone storage is nil")
	}
	err := s.engine.Destroy()
	log.Debug("Close the standalone storage")
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s == nil {
		return nil, errors.New("standalone storage is nil")
	}
	if s.engine == nil {
		return nil, errors.New("standalone storage not start")
	}

	txn := s.engine.Kv.NewTransaction(false)
	iters := make([]*engine_util.BadgerIterator, 0)
	reader := StandAloneStorageReader{txn, iters}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		default:
			return errors.New("unknown modify type")
		}
	}
	return nil
}

//	type StorageReader interface {
//		// When the key doesn't exist, return nil for the value
//		GetCF(cf string, key []byte) ([]byte, error)
//		IterCF(cf string) engine_util.DBIterator
//		Close()
//	}
type StandAloneStorageReader struct {
	txn   *badger.Txn
	iters []*engine_util.BadgerIterator
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, _ := engine_util.GetCFFromTxn(r.txn, cf, key)
	return value, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.txn)
	r.iters = append(r.iters, iter)
	return iter
}

func (r *StandAloneStorageReader) Close() {
	for _, iter := range r.iters {
		iter.Close()
	}
	r.txn.Discard()
}
