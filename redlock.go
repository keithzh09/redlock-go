package redlock

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	DefaultRetryTimes    = 3
	DefaultRetryInterval = time.Millisecond * 50
)

var (
	// script to extend the lock's lifetime
	prolongScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
	return 0
end
`

	deleteScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
else
	return 0
end
`
)

var (
	ErrLockRunning       = errors.New("the lock has been running")
	ErrLockFailed        = errors.New("lock failed, maybe the key has been locked, or there is not enough redis nodes")
	ErrHasNotEnoughNodes = errors.New("there is not enough valid nodes")
	ErrKeyValDisMatch    = errors.New("the value doesn't equal the key's current value")
)

type RedLock struct {
	retryTimes    int
	retryInterval time.Duration
	clients       []*redis.Client
	clientsCount  int

	ctx context.Context
	mtx sync.Mutex
	key string
	val string

	logger   Logger
	logLevel LogLevel
}

func NewRedLock(
	ctx context.Context,
	nodes []*redis.Client,
	retryTimes int,
	retryInterval time.Duration,
	logger Logger,
	logLevel LogLevel,
) (*RedLock, error) {
	l := &RedLock{
		retryTimes:    retryTimes,
		retryInterval: retryInterval,
		clients:       nodes,
		clientsCount:  len(nodes),
		logger:        logger,
		logLevel:      logLevel,
		mtx:           sync.Mutex{},
	}
	if l.retryTimes < 0 {
		l.retryTimes = DefaultRetryTimes
	}
	if l.retryInterval < 0 {
		l.retryInterval = DefaultRetryInterval
	}

	var valid int
	for _, node := range nodes {
		_, err := node.Ping(ctx).Result()
		addr, db := node.Options().Addr, node.Options().DB
		if err != nil {
			l.Warnf("redis client address %s, db: %d cannot connect, %s.", addr, db, err.Error())
		} else {
			l.Debugf("redis client address %s, db: %d is valid.", addr, db)
			valid += 1
		}
	}

	if valid < len(nodes)/2+1 {
		return nil, ErrHasNotEnoughNodes
	}

	return l, nil
}

func (l *RedLock) SetRetryCfg(retryTimes int, retryInterval time.Duration) {
	l.retryTimes = retryTimes
	l.retryInterval = retryInterval
}

func (l *RedLock) SetLogger(logger Logger) {
	l.logger = logger
}

// Lock the parameter ctx was used in lock and watchdog
func (l *RedLock) Lock(ctx context.Context, key string, expiration time.Duration) (err error) {
	succeeded := l.mtx.TryLock()
	if !succeeded {
		return ErrLockRunning
	}

	l.ctx = ctx
	l.key = key
	l.val = randomString(20)

	lock := func(client *redis.Client) error {
		var (
			set bool
			err error
		)
		for i := 0; i < l.retryTimes; i++ {
			set, err = client.SetNX(l.ctx, l.key, l.val, expiration).Result()
			if err == nil && set {
				return nil
			}
			if i != l.retryTimes-1 {
				time.Sleep(l.retryInterval)
			}
		}

		if err != nil {
			return err
		} else {
			return errors.New("setnx failed")
		}
	}

	ch := make(chan *redis.Client)
	for _, client := range l.clients {
		go func(client *redis.Client) {
			err := lock(client)
			if err == nil {
				l.Debugf("Key %s, val %s, client %s, lock successfully.", l.key, l.val, client.Options().Addr)
				ch <- client
			} else {
				l.Debugf("Key %s, val %s, client %s, failed to lock, %s.", l.key, l.val, client.Options().Addr, err)
				ch <- nil
			}
		}(client)
	}
	succeededC := make([]*redis.Client, 0)
	for i := 0; i < len(l.clients); i++ {
		client := <-ch
		if client != nil {
			succeededC = append(succeededC, client)
		}
	}

	if len(succeededC) >= len(l.clients)/2+1 {
		// set watchdog only for successful clients
		l.Infof("Key %s, val %s lock successfully, set watchdog.", key, l.val)
		go l.watchDog(l.ctx, succeededC, l.key, l.val, expiration)
		return nil
	} else {
		l.Infof("Key %s, val %s failed to lock, clear.", key, l.val)
		// for all clients
		go l.UnLock(l.ctx)
		return ErrLockFailed
	}
}

// UnLock nothing happens when failed to delete
func (l *RedLock) UnLock(ctx context.Context) {
	defer l.mtx.Unlock()

	unlock := func(client *redis.Client) error {
		script := deleteScript
		scriptKeys := []string{l.key}

		val, err := client.Eval(ctx, script, scriptKeys, l.val).Result()
		if err != nil {
			l.Debugf("redis client %s del key %s error, %s.", client.Options().Addr, l.key, err)
			return err
		}

		if val.(int64) != 1 {
			l.Debugf("redis client %s del key %s failed.", client.Options().Addr, l.key)
			return errors.New("failed")
		}
		return nil
	}

	var (
		wg       sync.WaitGroup
		delCount int32
	)
	wg.Add(l.clientsCount)
	for _, client := range l.clients {
		go func(client *redis.Client) {
			defer wg.Done()
			err := unlock(client)
			if err == nil {
				atomic.AddInt32(&delCount, 1)
			}
		}(client)
	}
	wg.Wait()
	l.Infof("Del key %s, val %v, successful client count %d.", l.key, l.val, delCount)
}

// watchDog stop when:
// 1. the ctx was done
// 2. successful clients count < (total clients count)/2+1
func (l *RedLock) watchDog(ctx context.Context, clients []*redis.Client, key string, val string, expiration time.Duration) {
	watchdog := func(client *redis.Client) error {
		var (
			res any
			err error
		)
		for i := 0; i < l.retryTimes; i++ {
			res, err = client.Eval(ctx, prolongScript, []string{key}, val, int(expiration.Seconds())).Result()
			if err == nil && res.(int64) != 0 {
				return nil
			}
			if i != l.retryTimes-1 {
				time.Sleep(l.retryInterval)
			}
		}
		if err != nil {
			return err
		} else {
			return errors.New("prolong failed")
		}
	}

	for {
		select {
		// 业务完成
		case <-ctx.Done():
			// 任务完成
			l.Infof("Key %s, val %s task finished, stop watchdog.", key, val)
			return
		default:
			// 自动续期
			l.Infof("Key %s, val %s task not finished, prolong the expiration, clients length %d.", key, val, len(clients))
			wg := sync.WaitGroup{}
			wg.Add(len(clients))
			var succeedCount int32
			for _, client := range clients {
				go func(client *redis.Client) {
					defer wg.Done()
					err := watchdog(client)
					if err == nil {
						atomic.AddInt32(&succeedCount, 1)
					}
				}(client)
			}
			wg.Wait()

			if int(succeedCount) < len(l.clients)/2+1 {
				l.Infof("Key %s, val %s prolong failed in verifying value, succeedCount %d, total clientCount %d, stop watchdog.",
					key, val, succeedCount, len(l.clients))
				return
			}
			time.Sleep(expiration / 2)
		}
	}
}

func (l *RedLock) Debugf(msg string, args ...any) {
	if l.logLevel <= LogLevelDebug {
		l.logger.Debugf(msg+"\n", args...)
	}
}

func (l *RedLock) Infof(msg string, args ...any) {
	if l.logLevel <= LogLevelInfo {
		l.logger.Infof(msg+"\n", args...)
	}
}

func (l *RedLock) Warnf(msg string, args ...any) {
	if l.logLevel <= LogLevelWarn {
		l.logger.Warnf(msg+"\n", args...)
	}
}

func (l *RedLock) Errorf(msg string, args ...any) {
	if l.logLevel <= LogLevelError {
		l.logger.Errorf(msg+"\n", args...)
	}
}

const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomString(n int) string {
	random := rand.NewSource(time.Now().UnixNano())
	sb := strings.Builder{}
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(charset[random.Int63()%int64(len(charset))])
	}
	return sb.String()
}
