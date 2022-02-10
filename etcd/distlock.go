package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Handler for locking
type RdDistLockHandlerT struct {
	logger logger.LoggerI
}

type DistLockResponse struct {
	Status  int              `json:"status"`
	LockKey string           `json:"lockKey"`
	Message string           `json:"message"`
	Error   string           `json:"error"`
	LeaseId clientv3.LeaseID `json:"leaseId"`
}

// Error codes are defined here
const (
	UnSuccessfulOp = iota + 601
	MtxCreationFail
	CliInitFail
)

// Interface
type RdDistLock interface {
	Setup()
	AcquireLock(ctx context.Context, lockKey string) DistLockResponse
	LoseLock(ctx context.Context, lockKey string, leaseId int64) DistLockResponse
}

func NewRdDistLockHandler() *RdDistLockHandlerT {
	return &RdDistLockHandlerT{}
}

func (lh *RdDistLockHandlerT) GetConnection() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   EndPoints[:],
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		// handle error!
		return nil, err
	}
	// defer cli.Close()
	return cli, nil
}

func (lh *RdDistLockHandlerT) Setup() {
	lh.logger = logger.NewLogger().Child("dlock")
}

func (lh *RdDistLockHandlerT) createMutex(lockKey string) (*concurrency.Mutex, *clientv3.Client, error) {
	client, cliErr := lh.GetConnection()
	if cliErr != nil {
		return nil, nil, cliErr
	}
	// defer client.Close()
	session, sessionErr := concurrency.NewSession(client)
	if sessionErr != nil {
		return nil, client, sessionErr
	}
	
  defer session.Close()
	mutex := concurrency.NewMutex(session, fmt.Sprintf(`/dlock-%v/`, lockKey))
	return mutex, client, nil
}

func (lh *RdDistLockHandlerT) AcquireLock(ctx context.Context, lockKey string) DistLockResponse {
	client, cliErr := lh.GetConnection()
	if cliErr != nil {
		return DistLockResponse{
			Status: CliInitFail,
			Error:  cliErr.Error(),
		}
	}
	// defer client.Close()
	session, sessionErr := concurrency.NewSession(client, concurrency.WithTTL(3600))
	if sessionErr != nil {
		return DistLockResponse{
			Status: 604,
			Error:  sessionErr.Error(),
		}
	}
	// defer session.Close()
	mutex := concurrency.NewMutex(session, fmt.Sprintf(`/dlock-%v`, lockKey))
	leaseId := session.Lease()
	// if err != nil {
	// 	return DistLockResponse{
	// 		Status: MtxCreationFail,
	// 		Error:  err.Error(),
	// 	}
	// }
	if lockErr := mutex.TryLock(ctx); lockErr != nil {
		return DistLockResponse{
			Status: UnSuccessfulOp,
			Error:  lockErr.Error(),
		}
	}
	return DistLockResponse{
		Status:  http.StatusOK,
		LockKey: mutex.Key(),
		Message: fmt.Sprintf(`lock has been acquired for key: %v`, lockKey),
		LeaseId: leaseId,
	}
}

func (lh *RdDistLockHandlerT) LoseLock(ctx context.Context, lockKey string, leaseId int64) DistLockResponse {
	client, cliErr := lh.GetConnection()
	if cliErr != nil {
		return DistLockResponse{
			Status: CliInitFail,
			Error:  cliErr.Error(),
		}
	}
	logger.Log.Infof(`Key from param: %v`, lockKey)
	kv := clientv3.NewKV(client)
	defer client.Close()
	leId := clientv3.LeaseID(leaseId)
	leResp, leErr := client.Lease.Revoke(ctx, leId)
	if leErr != nil {
		return DistLockResponse{
			Status: 605,
			Error:  leErr.Error(),
		}
	}
	leBytes, _ := json.Marshal(leResp)
	logger.Log.Infof(string(leBytes))
	delResp, delErr := kv.Delete(ctx, lockKey)

	if delErr != nil {
		return DistLockResponse{
			Status: MtxCreationFail,
			Error:  delErr.Error(),
		}
	}
	delBytes, _ := json.Marshal(delResp)
	logger.Log.Info(string(delBytes))
	return DistLockResponse{
		Status:  http.StatusOK,
		Message: fmt.Sprintf(`unlock for key: %v successful`, lockKey),
	}
}
