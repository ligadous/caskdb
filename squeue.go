package caskdb

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	ERR_NO_MORE_RECORDS = "SQueue: there is no values to be read."
	ERR_INVALID_RECORD  = "SQueue: invalid (nil) record."
	ERR_RECORD_INVALID  = "SQueue: invalid record (maybe already deleted?)"

	KEYS_BASE = 16 // changing this makes the version not backward compatible
)

// linked list of old records still in the queue
type ll_OldRecords struct {
	next *ll_OldRecords
	id   string
}

type ICache interface {
	Add(interface{}, time.Duration, ...string) error
	Delete(...string) error
	Retrieve(...string) (interface{}, error)
	Count() int
	Success() int
}

func (this *ll_OldRecords) Walk() bool {
	if this.next != nil {
		this.id = this.next.id
		this.next = this.next.next

		return true
	}

	return false
}

type SQueue struct {
	pq   *PQ
	keys map[string]bool

	max, current, countDisk, countRead, countDeleted uint64
	mu                                               sync.Mutex

	oldRecs       *ll_OldRecords
	oldRecsCount  uint64
	useLinkedList bool

	cache ICache
	ttl   time.Duration
}

func NewSQueue(path string) (*SQueue, error) {
	return new(SQueue).Init(path)
}

func (this *SQueue) Init(path string) (*SQueue, error) {
	var err error

	this.max = 0
	this.current = 0
	this.countRead = 0
	this.countDisk = 0
	this.countDeleted = 0

	// start pq
	this.pq, err = New(path)

	if err != nil {
		return nil, err
	}

	this.useLinkedList = false

	currentKeys := this.pq.ListAllKeys()

	if this.pq.lastOldKey != "" {
		this.max, _ = strconv.ParseUint(this.pq.lastOldKey, KEYS_BASE, 64)
		this.current = this.max
	}

	remainningRecords := make([]uint64, 0)

	for _, k := range currentKeys {
		i, _ := strconv.ParseUint(k, KEYS_BASE, 64)

		remainningRecords = append(remainningRecords, i)

		this.countDisk++
		this.oldRecsCount++
	}

	if len(remainningRecords) > 0 {
		bubbleSort(remainningRecords)

		this.oldRecs = new(ll_OldRecords)
		this.oldRecs.id = strconv.FormatUint(remainningRecords[0], KEYS_BASE)

		lenOldRecs := len(remainningRecords)

		o := this.oldRecs
		for i := 1; i < lenOldRecs; i++ {
			o.next = new(ll_OldRecords)
			o.next.id = strconv.FormatUint(remainningRecords[i], KEYS_BASE)
			o = o.next
		}

		this.useLinkedList = true
	}

	return this, nil
}

func (this *SQueue) GetCacheCount() int {
	if this.cache != nil {
		return this.cache.Count()
	}

	return -1
}

func (this *SQueue) GetStatus() string {
	var s string
	if this.useLinkedList {
		s = fmt.Sprintf("c: %s; m: %s (old records: %d - %s)", strconv.FormatUint(this.current, KEYS_BASE), strconv.FormatUint(this.max, KEYS_BASE), this.oldRecsCount, this.oldRecs.id)
	} else {
		s = fmt.Sprintf("c: %s; m: %s", strconv.FormatUint(this.current, KEYS_BASE), strconv.FormatUint(this.max, KEYS_BASE))
	}
	return s
}

func (this *SQueue) getNextReadKey() (string, error) {
	if this.current < this.max {
		this.current++
		return strconv.FormatUint(this.current, KEYS_BASE), nil
	}

	return "", errors.New(ERR_NO_MORE_RECORDS)
}

func (this *SQueue) getNextWriteKey() string {
	this.max++
	return strconv.FormatUint(this.max, KEYS_BASE)
}

func (this *SQueue) Push(rec []byte) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.countDisk++
	key := this.getNextWriteKey()

	if this.cache != nil {
		err := this.cache.Add(rec, this.ttl, key)

		if err != nil {
			return err
		}
	}

	return this.pq.Put(key, rec)
}

func (this *SQueue) retrieve(key string) ([]byte, error) {
	if this.cache != nil {
		i, err := this.cache.Retrieve(key)

		if err != nil {
			return nil, err
		}

		if i == nil {
			// trying to find key
			err := this.refresh(key)

			if err != nil {
				return nil, err
			}

			i, _ = this.cache.Retrieve(key)
		}

		if i != nil {
			_, ok := i.([]byte)
			if ok {
				return i.([]byte), nil
			}
		}
	}

	return this.pq.Get(key)
}

func (this *SQueue) refresh(currentReadKey string) error {
	var i uint64

	ikey, _ := strconv.ParseUint(currentReadKey, KEYS_BASE, 64)

	for i = 0; i < 200000; i++ {
		key := strconv.FormatUint(ikey+i, KEYS_BASE)

		rec, err := this.pq.Get(key)

		if err != nil || rec == nil || len(rec) == 0 {
			continue
		}

		this.cache.Add(rec, this.ttl, key)
	}

	return nil
}

func (this *SQueue) Pop() (rec []byte, rKey string, empty bool, err error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.useLinkedList {
		rKey = this.oldRecs.id

		rec, err = this.retrieve(rKey)

		if err != nil {
			return
		}

		if !this.oldRecs.Walk() {
			this.useLinkedList = false
			this.oldRecs = nil
		}

		this.oldRecsCount--
	} else {
		rKey, err = this.getNextReadKey()

		empty = (this.max == this.current)

		if err != nil {
			return
		}

		rec, err = this.retrieve(rKey)

		if err != nil {
			return
		}
	}

	this.countDisk--
	this.countRead++

	empty = (this.max == this.current)

	return
}

func (this *SQueue) Delete(k string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.cache != nil {
		this.cache.Delete(k)
	}

	if this.pq.IsKey(k) {
		this.countDeleted++
		return this.pq.Put(k, nil)
	}

	return errors.New(ERR_RECORD_INVALID)
}

func (this *SQueue) Length() string {
	return fmt.Sprintf("disk: %d; mem: %d", this.countDisk, this.countRead-this.countDeleted)
}

func (this *SQueue) CacheStatus() string {
	if this.cache != nil {
		return fmt.Sprintf("sz: %d; s: %d", this.cache.Count(), this.cache.Success())
	}

	return "no cache"
}

func (this *SQueue) MaxSize(m uint64) {
	this.pq.MSize = m
}

func (this *SQueue) Close() error {
	return this.pq.Close()
}

func (this *SQueue) SetMemCache(c ICache, ttl time.Duration, precache uint64) error {
	this.cache = c
	this.ttl = ttl

	if this.oldRecs != nil && precache > 0 {
		o := *(this.oldRecs)

		var i uint64

		for i = 0; i < precache; i++ {
			// pre-cache the precache first items. read all them now
			rec, err := this.pq.Get(o.id)

			if err != nil {
				return err
			}

			this.cache.Add(rec, this.ttl, o.id)

			if !o.Walk() {
				break
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}

func bubbleSort(d []uint64) {
	l := len(d)
	for c := 1; c < l; c++ {
		swapped := false

		for i := 1; i < l; i++ {
			if d[i-1] > d[i] {
				swapped = true
				d[i], d[i-1] = d[i-1], d[i]
			}
		}

		if !swapped {
			break
		}
	}
}
