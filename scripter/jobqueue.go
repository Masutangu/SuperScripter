package scripter

import (
	"github.com/garyburd/redigo/redis"
	"encoding/gob"
	"time"
	"bytes"
)


func (job *Job) serialize() string {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(*job)
	if err != nil {
		panic(err)
	}
	return buffer.String()
}

func deserializeJob(buffer *bytes.Buffer) *Job {
	var job Job
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&job)
	if err != nil {
		panic(err)
	}
	return &job
}

type JobQueue interface {
	enqueueJob(job *Job, worker_ip string) error
	dequeueJob(worker_ip string, timeout int) (*Job, error)
}

type RedisQueue struct {
	server 		redis.Conn
}

func NewRedisQueue(address string, auth string) RedisQueue {
	redis_server, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	if auth != "" {
		_, err = redis_server.Do("AUTH", auth)
		if err != nil {
			panic(err)
		}
	}	
	return RedisQueue{ server: redis_server}
}

func (rqueue RedisQueue) enqueueJob(job *Job, worker_ip string) error {
	job.StartTime = time.Now()
	_, err := rqueue.server.Do("LPUSH", worker_ip, job.serialize())
	return err
}

func (rqueue RedisQueue) dequeueJob(worker_ip string, timeout int) (*Job, error){
	rsp, err := redis.Strings(rqueue.server.Do("BRPOP", worker_ip, timeout))
	if err != nil {
		panic(err)
	}	
	return deserializeJob(bytes.NewBufferString(rsp[1])), nil
}
