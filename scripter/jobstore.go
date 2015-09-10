package scripter

import (
	"fmt"
	"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)
const (
	TASK_READY 		= 0
	TASK_FINISH 	= 1
	TASK_EXECUTING 	= 2
	TASK_FAILED		= 3
)

type Job struct {
	Id  		string
	Status 		int
	Command   	string
	Ret 		int
	StartTime	time.Time
}


func NewJob(id string, command string) *Job {
	return &Job{ Id: id, Status: TASK_READY, Command: command, Ret: 0 }
}

type JobStore interface{
	getFailedList() map[string]*Job 			//获取执行失败的任务列表
	getReadyList() map[string]*Job  			//获取未执行过的任务列表
	enqueueJob(job* Job, worker_ip string)  	//修改分发的任务的状态
	finishJob(job* Job, worker_ip string)       //修改已完成的任务的状态
	rescheduleJob(job_id string)				//将任务重新调度
	close()
}

type MysqlJobStore struct {
	db *sql.DB
}

func NewMysqlStore(address string ) MysqlJobStore {
	db, err := sql.Open("mysql", address)
	if err != nil {
		panic(err)
	}
	var sql string
	sql = "CREATE TABLE IF NOT EXISTS scripter_job (" +
		  "job_id VARCHAR(255) NOT NULL, " +
		  "command VARCHAR(255) NOT NULL, " +
		  "status INT(11), " +
		  "retry_time INT(11) DEFAULT 0, " +
		  "PRIMARY KEY(job_id) " +
		  ") DEFAULT CHARSET = utf8"
	
	_, err = db.Exec(sql)
	if err != nil {
		panic(err)
	}
	
	sql = "CREATE TABLE IF NOT EXISTS scripter_worker (" +
		  "worker_ip VARCHAR(255) NOT NULL, " +
		  "job_id	VARCHAR(255) NOT NULL, " +
		  "start_time DATETIME NOT NULL, " +
		  "end_time DATETIME DEFAULT NULL, " +
		  "ret INT(11) DEFAULT -1, " + 
		  "KEY job_id(job_id), " +
		  "CONSTRAINT scripter_woker_ibfk_1 FOREIGN KEY (job_id) REFERENCES scripter_job(job_id) " +
		  ") DEFAULT CHARSET = utf8"
	_, err = db.Exec(sql)
	if err != nil {
		panic(err)
	}
		
	return MysqlJobStore{db:db}
}

func (jobstore MysqlJobStore) getJobList(status int) map[string]*Job {
	var (
		id string
		command string
		job_list = make(map[string]*Job)
	)
	sql := fmt.Sprintf("select job_id, command from scripter_job where status = %d", status)
	fmt.Println(sql)
	rows, err := jobstore.db.Query(sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &command)
		if err != nil {
			panic(err)
		}
		job_list[id] = NewJob(id, command)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	} 
	return job_list
}

func (jobstore MysqlJobStore) getFailedList() map[string]*Job {
	return jobstore.getJobList(TASK_FAILED)
}

func (jobstore MysqlJobStore) getReadyList() map[string]*Job {
	return jobstore.getJobList(TASK_READY)
}

func (jobstore MysqlJobStore) enqueueJob(job* Job, worker_ip string) {
	sql := fmt.Sprintf("update scripter_job set status = %d where job_id = '%s'", TASK_EXECUTING, job.Id)
	fmt.Println(sql)
	_, err := jobstore.db.Exec(sql)
	if err != nil {
		panic(err)
	}
	
	sql = fmt.Sprintf("insert into scripter_worker(worker_ip, job_id, start_time) values ('%s', '%s', '%s')", 
						worker_ip, job.Id, job.StartTime.Format(time.RFC3339))
	fmt.Println(sql)
	_, err = jobstore.db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func (jobstore MysqlJobStore) finishJob(job *Job, worker_ip string) {
	var (
		job_sql string
		worker_sql string
	)
	if job.Ret == 0 {
		job_sql = fmt.Sprintf("update scripter_job set status = %d where job_id = '%s'", TASK_FINISH, job.Id)
	} else {
		job_sql = fmt.Sprintf("update scripter_job set status = %d, retry_time = retry_time + 1 where job_id = '%s'", 
			TASK_FAILED, job.Id)
	}
	fmt.Println(job_sql)
	_, err := jobstore.db.Exec(job_sql)
	if err != nil {
		panic(err)
	}
	worker_sql = fmt.Sprintf("update scripter_worker set end_time = now(), ret = %d where job_id = '%s' and worker_ip = '%s' and start_time = '%s'",
					 job.Ret, job.Id, worker_ip, job.StartTime.Format(time.RFC3339))
	fmt.Println(worker_sql)
	_, err = jobstore.db.Exec(worker_sql)
	if err != nil {
		panic(err)
	}
}

func (jobstore MysqlJobStore) rescheduleJob(job_id string) {
	sql := fmt.Sprintf("update scripter_job set status = %d where job_id = '%s'", TASK_READY, job_id)
	fmt.Println(sql)
	_, err := jobstore.db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func (jobstore MysqlJobStore) close() {
	jobstore.db.Close()
}