package scripter

import (
	"net"
	"net/rpc"
	"fmt"
	"os/exec"
	"time"
	"strings"
	gcfg "gopkg.in/gcfg.v1"
)



type Worker struct {
	job_queue 	JobQueue
	job_list	map[string]*Job 
	ip			string
	config 		Config
}

func getAddr() string {
	addrs, err := net.InterfaceAddrs()
    if err != nil {
        panic(err)
    }
    for k, addr := range addrs {
        fmt.Println("key=", k, "name=", addr.Network(), " value=", addr.String())
    }
	ip_port := strings.Split(addrs[2].String(), "/")
	return ip_port[0]
}

func (worker *Worker) Init(config_path string) {
	err := gcfg.ReadFileInto(&worker.config, config_path)
	if err != nil {
		panic(err)
	}
	
	worker.ip = getAddr()
	fmt.Println("ip=", worker.ip)
	worker.job_list = make(map[string]*Job)
	worker.job_queue = NewRedisQueue(worker.config.Common.JobQueueAddr, worker.config.Common.JobQueuePwd)
}

func (worker *Worker) DequeueJob(timeout int) *Job {
	job, err := worker.job_queue.dequeueJob(worker.ip, timeout)
	if err != nil {
		panic(err)
	}
	return job
}
	
func (worker *Worker) Start(config_path string) {
	worker.Init(config_path)
	go SendHeartBeat(worker.job_list, worker.ip, worker.config.Common.RpcAddr)
	for {
		job := worker.DequeueJob(0)
		worker.job_list[job.Id] = job
		go worker.processJob(job)
	}
}

func (worker *Worker) processJob(job *Job) {
	fmt.Println(time.Now(), " process job", job)
	ret := -1
	commands := strings.Split(job.Command, " ")
	cmd := exec.Command(commands[0], commands[1:]...)
	if err := cmd.Run(); err != nil {
		fmt.Println(time.Now(), "job [", job.Id, "] err=[", err, "]")
	} else {
		ret = 0
		fmt.Println(time.Now(), "job ", job.Id, " success")
	}
	worker.job_list[job.Id].Status = TASK_FINISH
	worker.job_list[job.Id].Ret = ret
}

func SendHeartBeat(job_list map[string]*Job, ip string, rpc_addr string) {
	client, err := rpc.Dial("tcp", rpc_addr)
	if err != nil {
		panic(err)
	}
	var monitor Monitor
	for {
		cpu_usage := monitor.MonitorCPU()
		fmt.Println("cpu usage:", cpu_usage)
		rpc_req := &HeartBeat{ WorkerIp: ip, CpuUsage: cpu_usage, MemoryUsage:  0.0 }
		
		for _, job := range job_list {
			if job.Status == TASK_FINISH {
				rpc_req.Joblist = append(rpc_req.Joblist, *job)
				delete(job_list, job.Id)
			}
		}
		var ret int
		err := client.Call("RPCServer.HeartBeat", rpc_req, &ret)
		if err != nil {
			panic(err)
		}
		time.Sleep( 5 * time.Second )
	}
}
