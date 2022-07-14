package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	dpCache "github.com/sanpj2292/rudder-dpcache-layer/dp_cache"
	"github.com/sanpj2292/rudder-dpcache-layer/etcd"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "9800"
	}
	// go func() {
	// 	c := make(chan os.Signal, 1)
	// 	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	// 	<-c
	// 	cancel()
	// }()

	// Run(ctx)
	g.Go(func() error {
		return startWebHandler(ctx, port)
	})

	err := g.Wait()
	if err != nil && err != context.Canceled {
		pkgLogger.Error(err)
		cancel()
	}
}

func startWebHandler(ctx context.Context, webport string) error {
	srvMux := mux.NewRouter()
	// srvMux.HandleFunc("/", standbyHealthHandler)
	// srvMux.HandleFunc("/health", standbyHealthHandler)
	srvMux.HandleFunc("/version", versionHandler)

	srvMux.HandleFunc("/bqstream", HandleBqstream).Methods(http.MethodGet)
	srvMux.HandleFunc("/gcs", HandleGcs).Methods(http.MethodPost)

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
		// Addr: ":" + webport,
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

	if err := http.ListenAndServe(fmt.Sprintf(":%s", webport), srvMux); err != nil {
		panic(err)
		// return fmt.Errorf("web server: %w", err)
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

func HandleBqstream(rw http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	ctx := context.Background()
	qParams := []string{"projectId", "datasetId", "tableId"}
	pkgLogger.Info("Inside BQStream method")
	fmt.Println("Inside BQStream method")
	qMap := make(map[string]string)
	for _, param := range qParams {
		value := query.Get(param)
		if value == "" {
			errMsg := fmt.Sprintf("%s field is not available", param)
			pkgLogger.Fatal(errMsg)
			Respond(rw, errMsg, http.StatusBadRequest)
			return
		}
		qMap[param] = value
	}
	pkgLogger.Infof("Creating bigquery client with projectId as %s", qMap["projectId"])
	bqClient, err := bigquery.NewClient(ctx, qMap["projectId"])
	if err != nil {
		pkgLogger.Errorf(`Error during BqClient creation operation: %+v`, err)
		Respond(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	bigQueryStr := fmt.Sprintf("select productId, count, productName from `%s.%s.%s` limit 10;",
		qMap["projectId"], qMap["datasetId"], qMap["tableId"])
	pkgLogger.Infof("Formed Query: %s", bigQueryStr)
	fmt.Printf("Formed Query: %s\n", bigQueryStr)
	bquery := bqClient.Query(bigQueryStr)
	pkgLogger.Info("Trying to read the query for BigQuery")
	it, err := bquery.Read(ctx)
	if err != nil {
		pkgLogger.Errorf(`Error after Query Read:- %+v`, err)
		Respond(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	pkgLogger.Infof(`Return from Read command: %+v`, it)
	pkgLogger.Infof(`Number of Rows fetched : %+v\n`, it.TotalRows)
	fmt.Println("Got the values from BigQuery successfully, processing it now.....")
	var retStr string
	for {
		// var values bigquery.Value
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		// valBytes, err = json.Marshal(&values)
		// if err != nil {
		// 	// TODO: Handle error.
		// 	pkgLogger.Errorf(`%+v`, err)
		// 	Respond(rw, err.Error(), http.StatusExpectationFailed)
		// 	return
		// }
		retStr += fmt.Sprintf(`%+v`, row)
	}
	pkgLogger.Info("About to send out the response for BigQuery")
	Respond(rw, retStr, http.StatusOK)
}

func HandleGcs(rw http.ResponseWriter, r *http.Request) {
	respData, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		pkgLogger.Fatalf(`[GCS] Problem in reading request body: %+v`, err)
		Respond(rw, "[GCS] Problem in reading request body", http.StatusBadRequest)
		return
	}
	projectId := gjson.GetBytes(respData, "projectId").String()

	ctx := context.Background()
	opts := []option.ClientOption{}
	credEnv := strings.TrimSpace(config.GetEnv("GOOGLE_APPLICATION_CREDENTIALS", ""))
	if credEnv != "" {
		credentials := gjson.GetBytes(respData, "credentials").String()
		opts = append(opts, option.WithCredentialsJSON([]byte(credentials)))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		pkgLogger.Fatalf(`[GCS] Problem in client creation: %+v`, err)
		Respond(rw, "[GCS] Problem in client creation", http.StatusBadRequest)
		return
	}
	defer client.Close()
	pkgLogger.Infof("[GCS] Before Buckets data-fetch for project: %+v", projectId)
	var bucketNames []string
	it := client.Buckets(ctx, projectId)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			pkgLogger.Fatalf(`[GCS] Problem while processing: %+v`, err)
			Respond(rw, "[GCS] Problem while processing the gcs buckets data", http.StatusBadRequest)
			return
		}
		bucketNames = append(bucketNames, battrs.Name)
	}
	pkgLogger.Infof("[GCS] After Buckets data-fetch for project: %+v", projectId)
	pkgLogger.Infof("Buckets: %+v", bucketNames)
	Respond(rw, strings.Join(bucketNames, ", "), http.StatusOK)
}
