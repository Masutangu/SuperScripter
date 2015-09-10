package scripter

import (
	"time"
)

type HeartBeat struct {
	WorkerIp	 	string
	Joblist   	 	[]Job
	CpuUsage 	 	float32
	MemoryUsage	float32
}

type Config struct {
	Common struct {
		JobQueueAddr string
		JobQueuePwd  string
		JobStoreAddr string
		RpcAddr string
	}
	Worker struct {
		MaximumEnqueueNumOnce int
	}
}

type WorkerStatus struct {
	ip					string
	cpu_usage 			float32     		//cpu使用率
	memory_usage 		float32   			//内存使用率
	pending_jobs	 	map[string]bool 	//执行任务列表
	last_update_time 	time.Time			//状态更新时间
	fresh_job_num		int 				//新收到心跳后 下发的任务数，用以限制一次下发的任务数量不要太多 
}

func NewWorker() *WorkerStatus {
	pending_jobs := make(map[string]bool)
	return &WorkerStatus{ pending_jobs: pending_jobs, fresh_job_num: 0 }
}


type WorkerList []*WorkerStatus

func (worker *WorkerStatus) enqueueJob(job *Job) {
	worker.pending_jobs[job.Id] = true
	worker.fresh_job_num += 1
}

func (worker *WorkerStatus) updateStatus(heartbeat *HeartBeat) {
	worker.ip = heartbeat.WorkerIp
	worker.cpu_usage = heartbeat.CpuUsage
	worker.memory_usage = heartbeat.MemoryUsage
	worker.last_update_time = time.Now()
	worker.fresh_job_num = 0
}

func (l WorkerList) Len() int {
    return len(l)
}

func (l WorkerList) Swap(i, j int) {
    l[i], l[j] = l[j], l[i]
}

func (l WorkerList) Less(i, j int) bool {
	var less bool
	var i_cpu = int(l[i].cpu_usage / 10)
	var j_cpu = int(l[j].cpu_usage / 10)
	
	switch {
		case i_cpu < j_cpu:
			less = true
		case i_cpu > j_cpu:
			less = false
		case i_cpu == j_cpu:
			less = (len(l[i].pending_jobs) < len(l[j].pending_jobs))
	}
	return less
}