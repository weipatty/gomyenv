package thread


import (
	"sync"
	//"fmt"
)

type Manager struct{
	worker_count	int
	running_count   int
	mutex      		sync.Mutex
}

func (this *Manager)SubWorker(){  
	this.mutex.Lock()
	this.worker_count -= 1
	//fmt.Println("count",this.worker_count)
	this.mutex.Unlock()
}
func (this *Manager)AddWorker(){  
	this.mutex.Lock()
	this.worker_count += 1
	//fmt.Println("count",this.worker_count)
	this.mutex.Unlock()
}
func (this *Manager)WorkerCount()int{  
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.worker_count
}

func (this *Manager)SubRunning(){  
	this.mutex.Lock()
	this.running_count -= 1
	this.mutex.Unlock()
}
func (this *Manager)AddRunning(){  
	this.mutex.Lock()
	this.running_count += 1
	this.mutex.Unlock()
}
func (this *Manager)RunningCount()int{  
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.running_count
}