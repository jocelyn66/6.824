package raft

import "log" // todo
import "sync"
// import "fmt"

// Debugging
const Debug = true // false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LogEntry struct {
	Command interface{}
	Term int
}

type AutoLockGuard struct {
    mu     *sync.Mutex
    locked bool
}

func NewAutoLockGuard(mu *sync.Mutex) *AutoLockGuard {
    mu.Lock()
    return &AutoLockGuard{
        mu:     mu,
        locked: true,
    }
}

func (g *AutoLockGuard) Unlock() {
    if g.locked {
        g.mu.Unlock()
        g.locked = false
    }
}

func (g *AutoLockGuard) AutoUnlock() {
    g.Unlock()
}

type SmartLockGuard struct {
    mu     *sync.Mutex
    locked bool
    name   string // 用于调试
}

func NewSmartLockGuard(mu *sync.Mutex, name string) *SmartLockGuard {
    mu.Lock()
    // fmt.Printf("[%s] Lock acquired\n", name)
    return &SmartLockGuard{
        mu:     mu,
        locked: true,
        name:   name,
    }
}

func (g *SmartLockGuard) Unlock() bool {
    if g.locked {
        g.mu.Unlock()
        g.locked = false
        //fmt.Printf("[%s] Lock released manually\n", g.name)
        return true
    }
    //fmt.Printf("[%s] Lock already released\n", g.name)
    return false
}

func (g *SmartLockGuard) AutoUnlock() {
    if g.locked {
        g.mu.Unlock()
        g.locked = false
        //fmt.Printf("[%s] Lock released automatically\n", g.name)
    }
}

func (g *SmartLockGuard) IsLocked() bool {
    return g.locked
}

