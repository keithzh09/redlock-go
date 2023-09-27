package redlock

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

type Log struct {
}

func (l *Log) Debugf(msg string, args ...any) {
	fmt.Printf("[debug] "+msg, args...)
}

func (l *Log) Infof(msg string, args ...any) {
	fmt.Printf("[info] "+msg, args...)
}

func (l *Log) Warnf(msg string, args ...any) {
	fmt.Printf("[warn] "+msg, args...)
}

func (l *Log) Errorf(msg string, args ...any) {
	fmt.Printf("[error] "+msg, args...)
}

var clients = []*redis.Client{
	redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}),
}

func TestNewRedLock(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	_, err := NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelDebug)
	if err != nil {
		t.Fatal(err)
	}

	clients := append(clients, redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:16379",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}))
	_, err = NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelDebug)
	if err == nil {
		t.Fatal("failed")
	}
}

func TestRedLock_Lock(t *testing.T) {
	key := "test-redlock"
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	rl, _ := NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelDebug)
	err := rl.Lock(ctx, key, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = rl.Lock(ctx, key, time.Second*5)
	if err == nil {
		t.Fatal("repeated lock failed")
	} else {
		t.Log("repeated lock pass")
	}
	time.Sleep(time.Second * 11)
}

func TestRedLock_UnLock(t *testing.T) {
	key := "test-redlock"
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	rl, _ := NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelDebug)
	err := rl.Lock(ctx, key, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = rl.Lock(ctx, key, time.Second*5)
	if errors.Is(err, ErrLockRunning) {
		t.Log("repeated lock pass")
	} else {
		t.Fatal("repeated lock failed")
	}
	time.Sleep(time.Second * 4)
	rl.UnLock(ctx)
	time.Sleep(time.Second * 11)
}

func TestRedLock_Lock2(t *testing.T) {
	key := "test-redlock"

	clients := append(clients, redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6380",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}), redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6381",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	f := func() {
		rl, _ := NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelDebug)
		err := rl.Lock(ctx, key, time.Second*5)
		if err != nil {
			if errors.Is(err, ErrLockFailed) {
				t.Log("lock failed case")
			} else {
				t.Fatal("failed, ", err)
			}
		}
	}

	go f()
	go f()
	go f()
	time.Sleep(time.Second * 1)
	go f()
	go f()
	time.Sleep(time.Second * 11)
}

func TestLogLevel(t *testing.T) {
	key := "test-redlock"

	clients := append(clients, redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6380",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}), redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6381",
		Password:     "",
		DB:           10,
		DialTimeout:  time.Second * 1,
		ReadTimeout:  time.Second * 1,
		WriteTimeout: time.Second * 1,
	}))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	f := func() {
		rl, _ := NewRedLock(ctx, clients, 2, -1, &Log{}, LogLevelInfo)
		err := rl.Lock(ctx, key, time.Second*5)
		if err != nil {
			if errors.Is(err, ErrLockFailed) {
				t.Log("lock failed case")
			} else {
				t.Fatal("failed, ", err)
			}
		}
	}

	go f()
	go f()
	go f()
	time.Sleep(time.Second * 1)
	go f()
	go f()
	time.Sleep(time.Second * 11)
}
