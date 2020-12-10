package cron

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type entryID int

type entry struct {
	ID entryID
	Schedule schedule
	Next time.Time
	Prev time.Time
	WrappedJob job
	Job job
}

func (this entry) Valid() bool {
	return this.ID != 0
}

type byTime []*entry

func (this byTime) Len() int {
	return len(this)
}
func (this byTime) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this byTime) Less(i, j int) bool {
	if this[i].Next.IsZero() {
		return false
	}

	if this[j].Next.IsZero() {
		return true
	}

	return this[i].Next.Before(this[j].Next)
}

type schedule interface {
	Next(time.Time) time.Time
}

type scheduleParser interface {
	Parse(spec string) (schedule, error)
}

type job interface {
	Run()
}

type jobWrapper func(job) job

type funcJob func()

func (this funcJob) Run() {
	this()
}

type chain struct {
	wrappers []jobWrapper
}

func (this chain) Then(j job) job {
	for i := range this.wrappers {
		j = this.wrappers[len(this.wrappers)-i-1](j)
	}

	return j
}

func newChain(c ...jobWrapper) chain {
	return chain{c}
}

type Config struct {
	Log string
	Schedules map[string]string
}

func (this *Config) Set(key string, value string) {
	this.Schedules[key] = value
}

type Cron struct {
	config *Config
	entries []*entry
	chain chain
	stop chan struct{}
	add chan *entry
	remove chan entryID
	running bool
	runningMu sync.Mutex
	parser scheduleParser
	nextID entryID
	jobWaiter sync.WaitGroup
	logMu sync.RWMutex
}

func (this *Cron) microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func (this *Cron) logger(message string) {
	if this.config == nil || this.config.Log == "" {
		return
	}

	now := time.Now()

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", now.Format("2006-01-02 15:04:05"), "(", this.microtime(), ") ", message, "\r\n"))

	go func(data string, that *Cron) {
		that.logMu.Lock()
		defer that.logMu.Unlock()

		file, _ := os.OpenFile(that.config.Log, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		file.WriteString(data)
	}(text.String(), this)
}

func (this *Cron) run() {
	this.logger("start")

	now := time.Now()
	for _, entry := range this.entries {
		entry.Next = entry.Schedule.Next(now)
		this.logger(fmt.Sprintf("%v:%v %v:%v %v:%v", "schedule now", now.Format("2006-01-02 15:04:05"), "entry", entry.ID, "next", entry.Next))
	}

	for {
		sort.Sort(byTime(this.entries))

		var timer *time.Timer
		if len(this.entries) == 0 || this.entries[0].Next.IsZero() {
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(this.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				this.logger(fmt.Sprintf("%v:%v", "wake now", now.Format("2006-01-02 15:04:05")))

				for _, e := range this.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					this.startJob(e.WrappedJob)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
					this.logger(fmt.Sprintf("%v:%v %v:%v %v:%v", "run now", now.Format("2006-01-02 15:04:05"), "entry", e.ID, "next", e.Next.Format("2006-01-02 15:04:05")))
				}

			case newEntry := <-this.add:
				timer.Stop()
				now = time.Now()
				newEntry.Next = newEntry.Schedule.Next(now)
				this.entries = append(this.entries, newEntry)
				this.logger(fmt.Sprintf("%v:%v %v:%v %v:%v", "added now", now.Format("2006-01-02 15:04:05"), "entry", newEntry.ID, "next", newEntry.Next.Format("2006-01-02 15:04:05")))

			case <-this.stop:
				timer.Stop()
				this.logger("stop")
				return

			case id := <-this.remove:
				timer.Stop()
				this.removeEntry(id)
				this.logger(fmt.Sprintf("%v %v:%v", "removed", "entry", id))
			}

			break
		}
	}
}

func (this *Cron) startJob(j job) {
	this.jobWaiter.Add(1)

	go func() {
		defer this.jobWaiter.Done()
		j.Run()
	}()
}

func (this *Cron) removeEntry(id entryID) {
	var entries []*entry
	for _, e := range this.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}

	this.entries = entries
}

func (this *Cron) Add(spec string, fun func()) *Cron {
	schedule, err := this.parser.Parse(spec)
	if err != nil {
		this.logger("add job error:" + err.Error())
		return this
	}

	cmd := funcJob(fun)

	this.runningMu.Lock()
	defer this.runningMu.Unlock()

	this.nextID++
	entry := &entry{
		ID: this.nextID,
		Schedule: schedule,
		WrappedJob: this.chain.Then(cmd),
		Job: cmd,
	}

	if !this.running {
		this.entries = append(this.entries, entry)
	} else {
		this.add <- entry
	}

	return this
}

func (this *Cron) Start() {
	this.runningMu.Lock()
	defer this.runningMu.Unlock()

	if this.running {
		return
	}

	this.running = true

	go this.run()
}

func (this *Cron) Stop() context.Context {
	this.runningMu.Lock()
	defer this.runningMu.Unlock()

	if this.running {
		this.stop <- struct{}{}
		this.running = false
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		this.jobWaiter.Wait()
		cancel()
	}()

	return ctx
}

func New(config *Config) *Cron {
	return &Cron{
		config: config,
		entries: nil,
		chain: newChain(),
		add: make(chan *entry),
		stop: make(chan struct{}),
		remove: make(chan entryID),
		running: false,
		runningMu: sync.Mutex{},
		parser: standardParser,
	}
}

var handler *Cron

func Init(config *Config) {
	handler = New(config)
}

func Add(spec string, fun func()) *Cron {
	return handler.Add(spec, fun)
}

func Stop()  {
	handler.Stop()
}