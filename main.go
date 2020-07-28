package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/samuel/go-zookeeper/zk"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

func redisworker() error {
	client := redis.NewClient(&redis.Options{
		Addr:     "10.23.14.65:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	var lockKey = "redis_lock"

	// lock
	resp := client.SetNX(lockKey, 1, time.Second * 5)
	result, err := resp.Result()

	if err != nil || !result {
		fmt.Println(err, "redis lock result: ", result)
		return err
	}

	log.Print("redis lock success")

	time.Sleep(time.Second)

	delResp := client.Del(lockKey)
	unlockresult, err := delResp.Result()
	if err == nil && unlockresult > 0 {
		println("redis unlock success")
	} else {
		println("redis unlock failed ", err)
	}

	log.Print("redis unlock success")

	return err
}

func etcdworker(key string) error {
	endpoints := []string{"10.23.14.65:2379"}
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	}

	cli, err := clientv3.New(cfg)
	if err != nil {
		log.Println("new cli error: ", err)
		return err
	}

	sess, err := concurrency.NewSession(cli)
	if err != nil {
		return err
	}

	m := concurrency.NewMutex(sess, "/"+key)

	err = m.Lock(context.TODO())
	if err != nil {
		log.Println("lock error:", err)
		return err
	}

	log.Print("etcd lock success")
	time.Sleep(time.Second * 1)

	err = m.Unlock(context.TODO())
	if err != nil {
		log.Println("unlock error:", err)
	}
	log.Print("etcd unlock success")

	return nil
}

func zkworker() error {
	c, _, err := zk.Connect([]string{"sr-dev-zk-cluster-1.gz.cvte.cn:2181"}, time.Second)
	if err != nil {
		return err
	}
	l := zk.NewLock(c, "/lock_zk", zk.WorldACL(zk.PermAll))
	err = l.Lock()
	if err != nil {
		return err
	}
	println("zk lock success ", )

	//do something
	time.Sleep(time.Second * 1)

	l.Unlock()
	println("zk unlock success")

	return err
}

func main() {

	goroutineCount :=  5
	//distributed lock by zk
	var wgzk sync.WaitGroup
	wgzk.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wgzk.Done()
			zkworker()
		}()
	}
	wgzk.Wait()

	//distributed lock by etcd
	var wgetcd sync.WaitGroup
	wgetcd.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wgetcd.Done()
			etcdworker("hello etcd")
		}()
	}
	wgetcd.Wait()

	//distributed lock by redis
	var wgredis sync.WaitGroup
	wgredis.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wgredis.Done()
			redisworker()
		}()
	}
	wgredis.Wait()
}
