package fsdump

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type DumpStatus struct {
	Err  error
	Path string
}

func ErrorLogCallback(status DumpStatus) {
	if status.Err != nil {
		log.Printf("failed to process file: %s: %v", status.Path, status.Err)
	}
}

type File struct {
	io.Reader
	io.Closer
	Path string
}

type FileSource interface {
	GetFile() *File
}

type ChannelFileSource struct {
	Chan chan File
}

func (source *ChannelFileSource) GetFile() *File {
	file, ok := <-source.Chan
	if !ok {
		return nil
	}
	return &file
}

type Dumper struct {
	Src        FileSource
	Dest       string
	NumWorkers int
	BufSize    int

	ThroughputMonitor io.Writer
	ProgressCallback  func(status DumpStatus)

	mu sync.Mutex
}

func (dumper *Dumper) Run() {
	if dumper.Src == nil {
		panic("FileSource cannot be nil")
	}

	numWorkers := dumper.NumWorkers
	if numWorkers < 1 {
		numWorkers = runtime.NumCPU()
	}

	bufSize := dumper.BufSize
	if bufSize < 1 {
		bufSize = 8192
	}

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for range numWorkers {
		worker := worker{
			dumper: dumper,
			buffer: make([]byte, bufSize),
		}
		go func() {
			worker.run()
			wg.Done()
		}()
	}

	wg.Wait()
}

type worker struct {
	dumper *Dumper
	buffer []byte
}

func (worker *worker) run() {
	for file := worker.dumper.Src.GetFile(); file != nil; file = worker.dumper.Src.GetFile() {
		err := worker.dumpFile(file)
		file.Close()

		if worker.dumper.ProgressCallback != nil {
			worker.dumper.mu.Lock()
			worker.dumper.ProgressCallback(DumpStatus{
				Err:  err,
				Path: file.Path,
			})
			worker.dumper.mu.Unlock()
		}
	}
}

func (worker *worker) dumpFile(in *File) error {
	dir, _ := filepath.Split(in.Path)
	if err := os.MkdirAll(filepath.Join(worker.dumper.Dest, dir), 0700); err != nil {
		return err
	}

	out, err := os.Create(filepath.Join(worker.dumper.Dest, in.Path))
	if err != nil {
		return err
	}
	defer out.Close()

	wr := io.Writer(out)
	if worker.dumper.ThroughputMonitor != nil {
		wr = io.MultiWriter(wr, worker.dumper.ThroughputMonitor)
	}

	_, err = io.CopyBuffer(wr, in, worker.buffer)
	return err
}
