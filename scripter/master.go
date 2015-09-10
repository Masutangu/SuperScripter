package scripter

import (
	"sort"
	"errors"
	"time"
	"net"
	"net/rpc"
	"fmt"
	gcfg "gopkg.in/gcfg.v1"
)

const (
	MAX_CPU_USAGE 		= 50
	MAX_MEMORY_USAGE	= 50
	MAXUINT 			= ^uint(0)
	TIMEOUT				= 60 * time.Second
)


type Master struct {
	job_queue 		JobQueue					//任务队列
	job_store		JobStore					//任务存储
	worker_map 	map[string]*WorkerStatus		//worker状态列表，以map形式存储
	config 			Config
}

func (master *Master) Init(config_path string) {
	err := gcfg.ReadFileInto(&master.config, config_path)
	if err != nil {
		panic(err)
	}
	master.job_queue = NewRedisQueue(master.config.Common.JobQueueAddr, master.config.Common.JobQueuePwd)
	master.worker_map = make(map[string]*WorkerStatus)
	master.job_store = NewMysqlStore(master.config.Common.JobStoreAddr)
}
/*
	挑选最空闲的worker来分发任务。
*/
func (master *Master) EnqueueJob(job *Job) error {
	selected_ip, err := master.selectAvailableWorker()
	if err == nil {
		//任务分发到任务队列里
		master.job_queue.enqueueJob(job, selected_ip)
		//更新worker的状态
		master.worker_map[selected_ip].enqueueJob(job)
		//更新在数据库里的状态 及下发分发记录
		master.job_store.enqueueJob(job, selected_ip) 
		fmt.Println(time.Now(), "master delivering job", job.Id, " to ", selected_ip)
	}
	return err
}

/*
	根据worker的cpu，未完成任务数来选择最空闲的机器
*/
func (master *Master) selectAvailableWorker() (string, error) {
	//go的map结构不支持sort，slice不支持remove，比较蛋疼。只好存的时候用map存，需要排序的时候放到slice。
	var candidate_list WorkerList
	for _, worker := range master.worker_map {
		//如果这轮分发该worker下发的任务数没有超过MAXIMUM_ENQUEUE_NUM_ONCE的话，列为候选列表
		if worker.fresh_job_num < master.config.Worker.MaximumEnqueueNumOnce {
			candidate_list = append(candidate_list, worker)
		}
	}
	if len(candidate_list) == 0 {
		return "", errors.New("No worker available currently")
	}
	//对所有worker进行排序
	sort.Sort(candidate_list)
	if (candidate_list[0].cpu_usage < MAX_CPU_USAGE) && (candidate_list[0].memory_usage < MAX_MEMORY_USAGE ) {
		return candidate_list[0].ip, nil
	} else {
		return "", errors.New("All worker busy currently")
	} 
}


func (master *Master) Start(config_path string) {
	master.Init(config_path)
	fmt.Println("scripter master running")
	heartbeat_channel := make(chan *HeartBeat)
	rpc_server := RPCServer{ heartbeat_channel }
	var job_list map[string]*Job
	//goroutine处理心跳包
	go DealWithRPC(rpc_server, master.config.Common.RpcAddr)
	
	for {
		// 重新下发失败的任务
		job_list = master.job_store.getFailedList()
		for _, job := range job_list {
			err := master.EnqueueJob(job)
			if err != nil {
				fmt.Println(err)
				break
			}
		}
		
		// 下发新的任务
		job_list = master.job_store.getReadyList()
		for _, job := range job_list {
			err := master.EnqueueJob(job)
			if err != nil {
				fmt.Println(err)
				break
			}
		}
		
		select {
			case heartbeat := <- heartbeat_channel:
			    fmt.Println(time.Now(), "deal with heartbeat channel", heartbeat)
				var worker *WorkerStatus
				//新的worker加入
				if master.worker_map[heartbeat.WorkerIp] == nil {
					fmt.Println("new worker ", heartbeat.WorkerIp, " has join")
					master.worker_map[heartbeat.WorkerIp] = NewWorker()
				}
				worker = master.worker_map[heartbeat.WorkerIp]
				worker.updateStatus(heartbeat)
			
				for _, job := range heartbeat.Joblist {
					if job.Status == TASK_FINISH {
						//任务已完成，从pending_jobs里删除
						delete(worker.pending_jobs, job.Id)
						master.job_store.finishJob(&job, heartbeat.WorkerIp)
					}
					//TODO 检查worker有没遗漏的任务
					/*
					else if job.Status == TASK_EXECUTING{
						
					}
					*/
				}
				//检查所有worker，如果超过timeout没有心跳包，认为该worker挂掉，将该worker下的任务重新分发给其他worker执行
				for ip, worker := range master.worker_map {
					if time.Now().Sub(worker.last_update_time) > TIMEOUT {
						fmt.Println("worker ip=", ip, " has disconnected")
						//挂掉的worker底下的任务修改状态为ready，重新分发。TODO:修改为批量方式
						for job_id, _ := range worker.pending_jobs {
							master.job_store.rescheduleJob(job_id)
						}
						//把该worker从worker_map里剔除
						delete(master.worker_map, ip)
					}
				}
				fmt.Println("finish rpc process")
				break
			case <- time.After( 10 * time.Second ):
				break
		}
	}
	master.job_store.close()
}

type RPCServer struct {
	rpc_channel chan *HeartBeat
}

func DealWithRPC(server RPCServer, rpc_addr string) {
	addr, err := net.ResolveTCPAddr("tcp", rpc_addr)
	if err != nil {
		panic(err)
	}
	inbound, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	rpc.Register(server)
	rpc.Accept(inbound)
}

func (server RPCServer) HeartBeat(heartbeat *HeartBeat, ret *int) error {
	fmt.Println("heart beat=", heartbeat.WorkerIp)
	server.rpc_channel <- heartbeat
	*ret = 0
	return nil
}

