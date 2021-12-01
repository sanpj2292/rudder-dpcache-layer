package dp_cache

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type DPCache interface {
	Setup()
	Create(ctx context.Context, key string, val interface{}, expTime float64) error
	Get(ctx context.Context, key string) (interface{}, error)
	// Delete() error
}

type DPCacheHandlerT struct {
	logger   logger.LoggerI
	rdClient *redis.Client
}

// This function creates a new OauthErrorResponseHandler
func NewDPCacheHandlerT() *DPCacheHandlerT {
	return &DPCacheHandlerT{}
}

func (cacheHandler *DPCacheHandlerT) Setup() {
	cacheHandler.logger = logger.NewLogger().Child("cache")
	cacheHandler.rdClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func (cache *DPCacheHandlerT) Create(ctx context.Context, key string, val interface{}, expTime float64) error {
	if err := cache.rdClient.Set(ctx, key, val, 0); err != nil {
		return err.Err()
	}
	return nil
}

func (cache *DPCacheHandlerT) Get(ctx context.Context, key string) (interface{}, error) {
	val, err := cache.rdClient.Get(ctx, key).Result()
	switch {
	case err == redis.Nil:
		return nil, fmt.Errorf("key does not exist")
	case err != nil:
		return nil, fmt.Errorf("Get failed", err)
	}
	return val, nil
}
