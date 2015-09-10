package scripter

import (
	"time"
	"github.com/shirou/gopsutil/cpu"
)

type Monitor int

func (monitor Monitor) MonitorCPU() float32 {
	cpu, err := cpu.CPUPercent(500 * time.Millisecond, false) 
	if err != nil {
		panic(err)
	}
	return float32(cpu[0])
}