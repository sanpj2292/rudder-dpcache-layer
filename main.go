package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	dpCache "github.com/sanpj2292/rudder-dpcache-layer/dp_cache"
	"github.com/sanpj2292/rudder-dpcache-layer/etcd"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

// Reason for using the used redis-client
// https://levelup.gitconnected.com/fastest-redis-client-library-for-go-7993f618f5ab
var (
	ReadTimeout             time.Duration
	ReadHeaderTimeout       time.Duration
	WriteTimeout            time.Duration
	IdleTimeout             time.Duration
	gracefulShutdownTimeout time.Duration
	MaxHeaderBytes          int
	pkgLogger               logger.LoggerI
	cache                   dpCache.DPCache
	etCache                 etcd.EtcdCache
	dlock                   etcd.RdDistLock
)

func loadConfig() {
	// config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	// config.RegisterBoolConfigVariable(true, &enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")

	config.RegisterDurationConfigVariable(time.Duration(0), &ReadTimeout, false, time.Second, []string{"ReadTimeOut", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(0), &ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(10), &WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(720), &IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(15), &gracefulShutdownTimeout, false, time.Second, "GracefulShutdownTimeout")
	config.RegisterIntConfigVariable(524288, &MaxHeaderBytes, false, 1, "MaxHeaderBytes")

}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("dpcacheLayer")
	etCache = etcd.NewEtcdCacheHandler()
	cache = dpCache.NewDPCacheHandlerT()
	dlock = etcd.NewRdDistLockHandler()
	cache.Setup()
	etCache.Setup()
}

func main() {
	Init()
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	// go func() {
	// 	c := make(chan os.Signal, 1)
	// 	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	// 	<-c
	// 	cancel()
	// }()

	// Run(ctx)
	g.Go(func() error {
		return startWebHandler(ctx)
	})

	err := g.Wait()
	if err != nil && err != context.Canceled {
		pkgLogger.Error(err)
		cancel()
	}
}

func startWebHandler(ctx context.Context) error {
	webPort := 9800
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/", standbyHealthHandler)
	srvMux.HandleFunc("/health", standbyHealthHandler)
	srvMux.HandleFunc("/version", versionHandler)

	// cache APIs
	srvMux.HandleFunc("/cache", func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			w.Write([]byte(fmt.Sprintf(`Error Reading the request body: %+v`, err)))
		}
		reqJson := string(payload)
		key := gjson.Get(reqJson, "key").String()
		val, getErr := cache.Get(ctx, key)
		if getErr != nil {
			w.Write([]byte(fmt.Sprintf(`Cannot get Value: %+v`, getErr)))
		}
		w.Write([]byte(fmt.Sprintf(`The Value for "%v" is %v`, key, val)))
	}).Methods("GET")

	srvMux.HandleFunc("/cache/create", func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			w.Write([]byte(fmt.Sprintf(`Error Reading the request body: %+v`, err)))
		}
		reqJson := string(payload)
		key := gjson.Get(reqJson, "key").String()
		val := gjson.Get(reqJson, "value").Value()
		expTime := gjson.Get(reqJson, "expirationTime").Float()
		// create in cache
		err = cache.Create(ctx, key, val, expTime)
		if err != nil {
			w.Write([]byte(
				fmt.Sprintf(`Insert Unsuccessful for key: %v with Error: %+v`, key, err)))
		}
		w.Write([]byte(fmt.Sprintf(`Created with key: %v`, key)))
	}).Methods("POST")

	srvMux.HandleFunc("/etcd/cache", func(rw http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			Respond(rw, fmt.Sprintf(`Error Reading the request body: %+v`, err), http.StatusInternalServerError)
		}
		reqJson := string(payload)
		key := gjson.Get(reqJson, "key").String()

		switch r.Method {
		case http.MethodDelete:
			delRes, delErr := etCache.Delete(ctx, key)
			if delErr != nil {
				respStCd := http.StatusInternalServerError
				if delErr.Error() == etcd.OpInProgress {
					respStCd = http.StatusAlreadyReported
				}
				Respond(rw, delErr.Error(), respStCd)
			}
			Respond(rw, delRes, http.StatusOK)
		case http.MethodGet:
			val, getErr := etCache.Get(ctx, key)
			if getErr != nil {
				Respond(rw, getErr.Error(), http.StatusInternalServerError)
			}
			Respond(rw, val, http.StatusOK)
		case http.MethodPost:
			val := gjson.Get(reqJson, "value").Value()
			expTime := gjson.Get(reqJson, "expirationTime").Int()
			res, putErr := etCache.Put(ctx, key, val, expTime)
			if putErr != nil {
				Respond(rw, putErr.Error(), http.StatusInternalServerError)
			}
			Respond(rw, fmt.Sprintf(`Inserted the value(key: "%v"): %v`, key, res), http.StatusCreated)
		}
	}).Methods(http.MethodDelete, http.MethodGet, http.MethodPost)

	// Specific flow for OAuth Destinations
	srvMux.HandleFunc("/oauth/token", HandleOAuthFlow).Methods(http.MethodPost)

	srvMux.HandleFunc("/dist/lock", HandleDistLock).Methods(http.MethodPost, http.MethodDelete)

	srv := &http.Server{
		Addr: ":" + strconv.Itoa(webPort),
		// Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       ReadTimeout,
		ReadHeaderTimeout: ReadHeaderTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
	defer func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	if err := http.ListenAndServe(":9800", srvMux); err != nil {
		return fmt.Errorf("web server: %w", err)
	}
	return nil
}

//StandbyHealthHandler is the http handler for health endpoint
func standbyHealthHandler(w http.ResponseWriter, r *http.Request) {
	appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", "DPCacheLayer"))
	healthVal := fmt.Sprintf(`{"appType": "%s", "mode":"%s"}`, appTypeStr, "Up")
	pkgLogger.Infof(`In StandByHealthHandler Function`)
	w.Write([]byte(healthVal))
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.Infof(`In VersionHandler Function`)
	w.Write([]byte("Version has not been defined yet"))
}

func Respond(writer http.ResponseWriter, retString string, statusCode int) {
	writer.WriteHeader(statusCode)
	writer.Write([]byte(retString))
}

func HandleOAuthFlow(rw http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		Respond(rw, fmt.Sprintf(`Error Reading the request body: %+v`, err), http.StatusInternalServerError)
	}
	var oauthParams etcd.OAuthFlowParams
	if unmErr := json.Unmarshal(payload, &oauthParams); unmErr != nil {
		Respond(rw, fmt.Sprintf(`Error in payload: %+v`, unmErr), http.StatusBadRequest)
	}
	authHeader := r.Header.Get("Authorisation")
	wspToken := strings.Split(authHeader, " ")[0]

	resp, resErr := etCache.FetchToken(r.Context(), &oauthParams, wspToken)
	if resErr != nil {
		Respond(rw, fmt.Sprintf(`Error Occurred while fetching token: %v`, resErr), http.StatusInternalServerError)
	}
	Respond(rw, resp, http.StatusOK)
}

func HandleDistLock(rw http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		Respond(rw, fmt.Sprintf(`Error Reading the request body: %+v`, err), http.StatusInternalServerError)
	}
	lockKey := gjson.Get(string(payload), "key").String()
	var resp etcd.DistLockResponse
	if r.Method == http.MethodPost {
		resp = dlock.AcquireLock(r.Context(), lockKey)
	} else if r.Method == http.MethodDelete {
		leaseId := gjson.Get(string(payload), "leaseId").Int()
		resp = dlock.LoseLock(r.Context(), lockKey, leaseId)
	}
	jsonRes, jsonErr := json.Marshal(resp)
	if jsonErr != nil {
		panic(jsonErr)
	}
	Respond(rw, string(jsonRes), http.StatusOK)
}
