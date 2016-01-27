package caskdb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	TIMETOFLUSH   = 5e9
	TIMETOROTATE  = 1e9
	TIMETOGARBAGE = 1e9
	MAXSIZE       = 100e6
	PQDIR         = "Data"
	BKPFOLDER     = ".bkp"
	PQFILENAME1   = "1"
	MAGICNUMBER   = "R3C"
	RECHEADER     = 15
	NUMESPACOS    = 2

	// at least 10% active records per block
	MIN_ACTIVE_KEYS_SHARE = 0.1
)

type RecFH struct {
	FH       *os.File
	FileTime time.Time
}

type Rec struct {
	File   string
	Offset *int64
}

//Queue structur
type PQ struct {
	Dir            string
	File           string // Current file
	MSize          uint64
	PoolFH         *map[string]*RecFH // Pool de open files
	Key            *map[string]*Rec   // set key or File to Offset
	CountFile      *map[string]int64  // Number of record in each file
	CurOffset      int64              // this is the file size too
	lastOldKey     string
	keysReallocate []string
	clearStarted   bool
	*sync.RWMutex
}

func New(dir string) (pq *PQ, err error) {
	if dir == "" {
		dir = PQDIR
	}
	var m sync.RWMutex

	err = os.MkdirAll(dir, 0766)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro MkdirAll %s", err))
	}

	// Backup
	err = os.MkdirAll(filepath.Join(dir, BKPFOLDER), 0766)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro MkdirAll %s", err))
	}

	// Num File -> FileHandle
	p := make(map[string]*RecFH)
	// Keys -> offset
	k := make(map[string]*Rec)
	// Count Files
	c := make(map[string]int64)

	//Start a DB
	pq = &PQ{dir, "1", MAXSIZE, &p, &k, &c, 0, "", nil, false, &m}

	// Number files
	d, err := ReadDir(dir)
	files := make([]string, 0)

	if err == nil {
		if len(d) > 0 {
			for _, f := range d {
				if f.Name() != BKPFOLDER {
					files = append(files, f.Name())
				}
			}
		}
	}

	if len(files) > 0 {
		for _, fname := range files {
			err := pq.Start(fname)

			if err != nil {
				return nil, err
			}
		}
	} else {
		err := pq.Start("1")

		if err != nil {
			return nil, err
		}
	}

	err = pq.Reallocate()

	if err != nil {
		pq.Close()
		return nil, err
	}

	// Flush Storage
	go pq.Flush()

	// Rotate file blocks with MaxSize
	go pq.Rotate()

	// Garbage colector
	go pq.Garbage()

	return pq, nil
}

func (pq *PQ) MaxSize(m uint64) {
	pq.MSize = m
}

// Disk Storage
func (pq *PQ) Open() error {
	var err error

	FH, err := os.OpenFile(filepath.Join(pq.Dir, pq.File), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro ao abrir %s) %s", pq.File, err))
	}

	pool := *pq.PoolFH
	pool[pq.File] = &RecFH{FH, time.Now()}

	return nil
}

// Close FH
func (pq *PQ) Close() error {
	var err error

	for _, v := range *pq.PoolFH {
		if v.FH != nil {
			err = v.FH.Close()
		}
	}

	return err
}

// Flush Disk
func (pq *PQ) Flush() {
	for {
		time.Sleep(TIMETOFLUSH)

		p := *pq.PoolFH
		if p == nil {
			continue
		}

		if p[pq.File] != nil && p[pq.File].FH != nil {
			p[pq.File].FH.Sync()
		}
	}
}

// Rotate
func (pq *PQ) Rotate() {
	for {
		time.Sleep(TIMETOROTATE)

		pq.checkNewBlock()
	}
}

func (pq *PQ) checkNewBlock() {
	if pq.CurOffset > int64(pq.MSize) {
		numFile, _ := strconv.Atoi(pq.File)
		nextFile := numFile + 1
		pq.Start(fmt.Sprintf("%d", nextFile))
	}
}

func (pq *PQ) Reallocate() error {
	if pq.keysReallocate != nil && len(pq.keysReallocate) > 0 {
		memkey := *pq.Key

		for _, key := range pq.keysReallocate {
			if memkey[key] != nil && memkey[key].File != pq.File {
				rec, err := pq.Get(key)

				if err == nil && rec != nil {
					err := pq.Put(key, rec)

					if err != nil {
						return err
					}

					pq.checkNewBlock()
				}
			}
		}
	}

	pq.keysReallocate = nil
	pq.checkGarbage()
	return nil
}

// Load all keys to memory
func (pq *PQ) Start(f string) error {
	pq.Lock()
	defer pq.Unlock()

	// next file
	pq.File = f

	err := pq.Open()
	if err != nil {
		return err
	}

	curoffset := int64(0)
	memkey := *pq.Key
	countF := *pq.CountFile

	block_keys := map[string]bool{}
	total_keys := 0

	for {
		offsetTemp := int64(0)
		key, data, offset, err := pq.GetOff(curoffset, pq.File)
		if err != nil || key == nil {
			break
		} else {
			skey := string(key)
			pq.lastOldKey = skey
			if len(data) > 0 {
				total_keys++

				offsetTemp = curoffset

				if memkey[skey] == nil {
					memkey[skey] = &Rec{pq.File, &offsetTemp}
				} else {
					countF[memkey[skey].File]--
					memkey[skey] = &Rec{pq.File, &offsetTemp}
				}
				countF[f]++

				block_keys[skey] = true
			} else {
				if memkey[skey] != nil {
					countF[memkey[skey].File]--
					delete(memkey, skey)

					block_keys[skey] = false
				}
			}
			curoffset = offset
		}
	}

	count_active_keys := 0

	for _, b := range block_keys {
		if b {
			count_active_keys++
		}
	}

	rate := float64(count_active_keys) / float64(total_keys)

	if rate < MIN_ACTIVE_KEYS_SHARE {
		if pq.keysReallocate == nil {
			pq.keysReallocate = make([]string, 0, total_keys)
		}

		for k, active := range block_keys {
			if active {
				pq.keysReallocate = append(pq.keysReallocate, k)
			}
		}
	}

	pq.CurOffset = curoffset
	return nil
}

func (pq *PQ) GetKeysReallocate() []string {
	return pq.keysReallocate
}

//Garbage Colector
func (pq *PQ) Garbage() {
	for {
		time.Sleep(TIMETOGARBAGE)

		pq.checkGarbage()
	}
}

func (pq *PQ) checkGarbage() {
	cpCountFile := make(map[string]int64)

	pq.Lock()
	for k, v := range *pq.CountFile {
		cpCountFile[k] = v
	}
	pq.Unlock()

	for k, v := range cpCountFile {
		if pq.File != k && v == 0 {
			// Fecha e Move para bkp
			pq.Lock()

			delete(*pq.CountFile, k)

			poolf := *pq.PoolFH
			recFH := poolf[k]

			if recFH != nil && recFH.FH != nil {
				recFH.FH.Close()
			}

			delete(poolf, k)
			pq.Unlock()
			bkpFile := filepath.Join(pq.Dir, BKPFOLDER, k)
			os.Rename(filepath.Join(pq.Dir, k), bkpFile)
			go func() {
				if err := exec.Command(`gzip`, bkpFile).Run(); err != nil {
					fmt.Println("PrioriQueue.Garbage: failed gzip:", err, bkpFile)
				}
			}()
		}
	}
}

// Insert key-value
func (pq *PQ) Put(key string, data []byte) error {
	shrinkByteSlice(&data)

	if key == "" || pq == nil {
		return errors.New("Key not exist [1]")
	}

	pq.Lock()

	memkey := *pq.Key

	if data != nil && memkey[key] != nil && memkey[key].File != pq.File {
		pq.Unlock()
		pq.Put(key, nil)
		pq.Lock()
	}

	defer pq.Unlock()

	// No delete data set as nil
	if data == nil && memkey[key] == nil {
		return errors.New("Key not exist")
	}

	LenKey := fmt.Sprintf("%d", len(key))
	SzLenKey := fmt.Sprintf("%d", len(LenKey))
	LenData := fmt.Sprintf("%d", len(data))
	SzLenData := fmt.Sprintf("%d", len(LenData))

	var buf bytes.Buffer
	buf.Write([]byte(MAGICNUMBER))
	buf.Write(makeCheckSum(&data))
	buf.Write([]byte(SzLenKey))
	buf.Write([]byte(SzLenData))
	buf.Write([]byte(LenKey))
	buf.Write([]byte(LenData))
	buf.Write([]byte(" "))
	buf.Write([]byte(key))
	buf.Write([]byte(" "))
	buf.Write([]byte(data))
	buf.Write([]byte("\n"))

	// Save to storage
	var err error
	pool := *pq.PoolFH
	offtemp := pq.CurOffset

	if data != nil {
		_, err = pool[pq.File].FH.Write(buf.Bytes())
	} else {
		_, err = pool[memkey[key].File].FH.Write(buf.Bytes())
	}

	if data != nil || (memkey[key] != nil && memkey[key].File == pq.File) {
		pq.CurOffset += int64(buf.Len())
	}

	if err == nil {
		countF := *pq.CountFile

		if data != nil {
			if memkey[key] != nil {
				countF[memkey[key].File]--
			}

			memkey[key] = &Rec{pq.File, &offtemp}
			countF[pq.File]++
		} else {
			countF[memkey[key].File]--
			delete(memkey, key)
		}
	}

	buf.Reset()
	return err
}

func (pq *PQ) Get(key string) ([]byte, error) {
	pq.Lock()
	defer pq.Unlock()

	mkey := *pq.Key
	if mkey[key] == nil {
		return nil, errors.New("Key not exist [2]")
	}

	offset := mkey[key].Offset
	file := mkey[key].File

	if offset == nil {
		return nil, errors.New("Key not exist [3]")
	}

	_, data, _, err := pq.GetOff(*offset, file)
	return data, err
}

func (pq *PQ) GetOff(offset int64, file string) ([]byte, []byte, int64, error) {
	pool := *pq.PoolFH
	FH := pool[file].FH

	_, err := FH.Seek(offset, 0)
	if err != nil {
		return nil, nil, 0, err
	}

	// Read bytes from storage
	var dataHeader []byte = make([]byte, RECHEADER)
	szHeader, err := FH.Read(dataHeader)
	if err != nil {
		return nil, nil, 0, err
	}

	// Size of key and value
	SzLenKey, _ := strconv.Atoi(string(dataHeader[13:14]))
	SzLenData, _ := strconv.Atoi(string(dataHeader[14:15]))

	var dataLen []byte = make([]byte, SzLenKey+SzLenData)

	szLen, err := FH.Read(dataLen)
	if err != nil {
		return nil, nil, 0, err
	}

	LenKey, _ := strconv.Atoi(string(dataLen[0:SzLenKey]))
	LenData, _ := strconv.Atoi(string(dataLen[SzLenKey:]))

	//Read data
	szTotal := LenKey + LenData + NUMESPACOS
	var data []byte = make([]byte, szTotal)
	szData, err := FH.Read(data)

	if err != nil {
		return nil, nil, 0, err
	}

	nextOffset := offset + int64(szHeader+szLen+szData+1)

	posData := LenKey + NUMESPACOS
	posKey := LenKey + 1

	recData := data[posData:]
	if !bytes.Equal(dataHeader[3:13], makeCheckSum(&recData)) {
		return nil, nil, 0, fmt.Errorf("caskdb: failed checksum %s", string(dataHeader[3:13]))
	}

	return data[1:posKey], recData, nextOffset, nil
}

func (pq *PQ) ListAllKeys() []string {
	var skeys []string
	for k, _ := range *pq.Key {
		skeys = append(skeys, k)
	}
	return skeys
}

func (pq *PQ) ListKeys(p string) []string {
	var skeys []string
	l := len(p)
	for k, _ := range *pq.Key {
		if l > 0 {
			if len(k) >= l {
				pk := k[0:l]
				if pk != p {
					continue
				}
			} else {
				continue
			}
		}
		skeys = append(skeys, k)
	}
	return skeys
}

func (pq *PQ) CountKeys(p string) int64 {
	var count int64
	l := len(p)
	for k, _ := range *pq.Key {
		if l > 0 {
			if len(k) > l {
				pk := k[0:l]
				if pk != p {
					continue
				}
			} else {
				continue
			}
		}
		count++
	}
	return count
}

func (pq *PQ) IsKey(key string) bool {
	mkey := *pq.Key
	if mkey[key] == nil {
		return false
	}
	return true
}

func (pq *PQ) SetCleanerTime(d time.Duration) {
	if !pq.clearStarted {
		pq.clearStarted = true
		go pq.cleanerProcess(d)
	}
}

func (pq *PQ) cleanerProcess(d time.Duration) {
	pathbkp := filepath.Join(pq.Dir, BKPFOLDER)
	for {
		dirs, err := ioutil.ReadDir(pathbkp)
		if err != nil {
			// not found dirs
			time.Sleep(time.Hour)
			continue
		}
		for _, dir := range dirs {
			if !dir.IsDir() {
				if time.Since(dir.ModTime()) > d {
					os.Remove(dir.Name())
				}
			}
		}
		time.Sleep(time.Hour * 24)
	}
}

type byInt []os.FileInfo

func (f byInt) Len() int { return len(f) }

func (f byInt) Less(i, j int) bool {
	prim, _ := strconv.Atoi(f[i].Name())
	sec, _ := strconv.Atoi(f[j].Name())
	return prim < sec
}

func (f byInt) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

func ReadDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Sort(byInt(list))
	return list, nil
}

func makeCheckSum(dat *[]byte) []byte {
	return []byte(fmt.Sprintf("%10d", crc32.ChecksumIEEE(*dat)))[:10]
}

func shrinkByteSlice(d *[]byte) {
	if len(*d) < cap(*d) {
		var b bytes.Buffer
		b.Write(*d)
		*d = b.Bytes()
	}
}
