package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	EndPoints = [...]string{"localhost:2379", "localhost:22379", "localhost:32379"}
)

const (
	OpInProgress = `some-other operation is in-progress`
)

type EtcdCacheHandlerT struct {
	logger logger.LoggerI
	tr     *http.Transport
	client *http.Client
	dlock  RdDistLock
}

type EtcdCache interface {
	Setup()
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key string, value interface{}, expTimeInSecs int64) (string, error)
	Delete(ctx context.Context, key string) (string, error)

	// OAuth Flow
	FetchToken(ctx context.Context, oauthParams *OAuthFlowParams, workspaceToken string) (string, error)
}

type OAuthFlowParams struct {
	HasExpired  bool   `json:"hasExpired"`
	AccessToken string `json:"accessToken"`
	RequestId   string `json:"requestId"`
	RequestType string `json:"requestType"`
	WorkspaceId string `json:"workspaceId"`
}

type ControlPlaneRequestT struct {
	Body           string
	ContentType    string
	Url            string
	Method         string
	destName       string
	RequestType    string // This is to add more refined stat tags
	WorkspaceToken string
}

type OAuthTokenBodyParams struct {
	HasExpired   bool   `json:"hasExpired"`
	ExpiredToken string `json:"expiredToken"`
}

func NewEtcdCacheHandler() *EtcdCacheHandlerT {
	return &EtcdCacheHandlerT{}
}

func (handler *EtcdCacheHandlerT) GetConnection() (*clientv3.Client, error) {
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

func (cacheHandler *EtcdCacheHandlerT) Setup() {
	cacheHandler.logger = logger.NewLogger().Child("etcd_cache")
	cacheHandler.tr = &http.Transport{}
	//This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
	cacheHandler.client = &http.Client{}
	cacheHandler.dlock = NewRdDistLockHandler()
}

func (handler *EtcdCacheHandlerT) Put(ctx context.Context, key string, value interface{}, expTimeInSecs int64) (string, error) {
	if expTimeInSecs < 0 {
		return "", fmt.Errorf("invalid argument expTimeInSecs provided")
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	client, cliErr := handler.GetConnection()
	if cliErr != nil {
		return "", cliErr
	}
	defer client.Close()
	session, sessionErr := concurrency.NewSession(client)
	if sessionErr != nil {
		return "", sessionErr
	}
	defer session.Close()
	kv := clientv3.NewKV(client)
	mutex := concurrency.NewMutex(session, fmt.Sprintf(`/dlock-%v`, key))

	lockErr := mutex.TryLock(ctx)
	if lockErr != nil {
		return "", lockErr
	}
	availableValue, getErr := handler.Get(ctx, key)
	if getErr != nil {
		return "", getErr
	}
	if availableValue == fmt.Sprintf(`%v`, value) {
		// Retry unlocking maybe
		unlockErr := mutex.Unlock(ctx)
		if unlockErr != nil {
			return "", unlockErr
		}
		return availableValue, nil
	}

	var grantRes *clientv3.LeaseGrantResponse
	var grantErr, putErr error
	var resp *clientv3.PutResponse
	if expTimeInSecs > 0 {
		// With expiration
		grantRes, grantErr = client.Grant(ctx, expTimeInSecs)
		if grantErr != nil {
			return "", grantErr
		}
		resp, putErr = kv.Put(ctx, key, string(jsonBytes), clientv3.WithLease(grantRes.ID))
	} else {
		// Wihtout expiration
		resp, putErr = kv.Put(ctx, key, string(jsonBytes))
	}
	unlockErr := mutex.Unlock(ctx)
	if unlockErr != nil {
		return "", unlockErr
	}

	byteResp, mErr := json.Marshal(resp)
	if mErr != nil {
		panic(mErr)
	}
	return string(byteResp), putErr
}

func (handler *EtcdCacheHandlerT) Get(ctx context.Context, key string) (string, error) {
	client, cliErr := handler.GetConnection()
	if cliErr != nil {
		return "", cliErr
	}
	defer client.Close()
	kv := clientv3.NewKV(client)
	resp, getErr := kv.Get(ctx, key, clientv3.WithLastRev()...)
	if len(resp.Kvs) <= 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), getErr
}

func (handler *EtcdCacheHandlerT) Delete(ctx context.Context, key string) (string, error) {
	client, cliErr := handler.GetConnection()
	if cliErr != nil {
		return "", cliErr
	}
	defer client.Close()
	session, sessionErr := concurrency.NewSession(client)
	if sessionErr != nil {
		return "", sessionErr
	}
	defer session.Close()
	kv := clientv3.NewKV(client)
	mutex := concurrency.NewMutex(session, fmt.Sprintf(`/dlock-%v`, key))
	if lockErr := mutex.TryLock(ctx); lockErr != nil {
		return "", errors.New(`some-other operation is in-progress`)
	}
	delRes, delErr := kv.Delete(ctx, key)
	if delErr != nil {
		return "", delErr
	}
	return fmt.Sprintf(`Deleted: %v`, delRes.Deleted), nil
}

func (handler *EtcdCacheHandlerT) FetchToken(ctx context.Context, oauthParams *OAuthFlowParams, workspaceToken string) (string, error) {
	client, cliErr := handler.GetConnection()
	if cliErr != nil {
		return "", cliErr
	}
	defer client.Close()
	session, sessionErr := concurrency.NewSession(client)
	if sessionErr != nil {
		return "", sessionErr
	}
	defer session.Close()
	mutex := concurrency.NewMutex(session, fmt.Sprintf(`/dlock-%v`, oauthParams.RequestId))
	var cacheToken string
	var getErr error
	if lockErr := mutex.TryLock(ctx); lockErr != nil {
		if cacheToken, getErr = handler.Get(ctx, oauthParams.RequestId); getErr != nil {
			mutex.Unlock(ctx)
			return "", getErr
		}
		if cacheToken != "" && cacheToken != oauthParams.AccessToken {
			mutex.Unlock(ctx)
			return cacheToken, nil
		}
	}
	logger.Log.Info("Refresh Token Request Starts")
	// Actual TokenFetch from config-backend
	refreshUrl := fmt.Sprintf("%s/dest/workspaces/%s/accounts/%s/token", getConfigBEUrl(), oauthParams.WorkspaceId, oauthParams.RequestId)
	res, err := json.Marshal(&OAuthTokenBodyParams{
		HasExpired:   oauthParams.HasExpired,
		ExpiredToken: oauthParams.AccessToken,
	})
	if err != nil {
		panic(err)
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:      http.MethodPost,
		Url:         refreshUrl,
		ContentType: "application/json; charset=utf-8",
		Body:        string(res),
		destName:    "BQSTREAM",
		RequestType: oauthParams.RequestType,
	}
	_, retVal := handler.cpApiCall(refreshCpReq)
	mutex.Unlock(ctx)
	handler.Put(ctx, oauthParams.RequestId, retVal, 0)
	return retVal, nil
}

func getConfigBEUrl() string {
	return config.GetEnv("CONFIG_BE_URL", "http://localhost:5000")
}

func processResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		if ioUtilReadErr != nil {
			return http.StatusInternalServerError, ioUtilReadErr.Error()
		}
	}
	//Detecting content type of the respData
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}

func (handler *EtcdCacheHandlerT) cpApiCall(cpReq *ControlPlaneRequestT) (int, string) {
	var reqBody *bytes.Buffer
	var req *http.Request
	var err error
	if cpReq.Body != "" {
		reqBody = bytes.NewBufferString(cpReq.Body)
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, reqBody)
	} else {
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, nil)
	}
	if err != nil {
		handler.logger.Errorf("[%s request] :: destination request failed: %+v\n", "OauthFlow", err)
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	// Authorisation setting
	// Put a hard-coded Token for now
	req.SetBasicAuth(cpReq.WorkspaceToken, "")

	// Set content-type in order to send the body in request correctly
	if cpReq.ContentType != "" {
		req.Header.Set("Content-Type", cpReq.ContentType)
	}

	// cpApiDoTimeStart := time.Now()
	res, doErr := handler.client.Do(req)
	// stats.NewTaggedStat("cp_request_latency", stats.TimerType, stats.Tags{
	// 	"url":         cpReq.Url,
	// 	"destination": cpReq.destName,
	// 	"requestType": cpReq.RequestType,
	// }).SendTiming(time.Since(cpApiDoTimeStart))
	handler.logger.Debugf("[%s request] :: destination request sent\n", "OauthFlow")
	if res.Body != nil {
		defer res.Body.Close()
	}
	if doErr != nil {
		// Abort on receiving an error
		handler.logger.Errorf("[%s request] :: destination request failed: %+v\n", "OauthFlow", doErr)
		return http.StatusBadRequest, doErr.Error()
	}
	statusCode, resp := processResponse(res)
	return statusCode, resp
}
