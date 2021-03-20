package main

import (
	"crypto/md5"
	"find-duplicates/set"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
)

var (
	path         *string
	fileHashes   = set.NewStringStringSet()
	duplicates   = set.NewStringSliceSet()
	dirsWG       = new(sync.WaitGroup)
	stop         = make(chan struct{}, 1)
	files_buffer = 100
)

func init() {
	path = flag.String("path", "data", "path to the directory where duplicates will be searched")
	flag.Parse()
}

func main() {
	fileNames := make(chan string, files_buffer)
	dirsDone := make(chan struct{}, 1)
	filesDone := make(chan struct{}, 1)
	errors := make(chan error, 1)
	defer func() {
		close(fileNames)
		close(dirsDone)
		close(errors)
		close(stop)
	}()
	dirsWG.Add(1)
	go ReadDir(*path, fileNames, errors)
	go RunFiles(fileNames, errors, dirsDone, filesDone)

	go func() {
		for true {
			select {
			case err := <-errors:
				if err != nil {
					log.Printf("error: %+v", err)
					stop <- struct{}{}
				}
			default:
				runtime.Gosched()
			}
		}
	}()
	dirsWG.Wait()
	fmt.Println("Dirs done!")
	dirsDone <- struct{}{}
Loop:
	for true {
		select {
		case <-stop:
			fmt.Println("Loop, stop")
			stop <- struct{}{}
			return
		case <-filesDone:
			fmt.Println("Loop, break")
			break Loop
		default:
			runtime.Gosched()
		}
	}
	duplicates.Print()
}

func ReadDir(dirName string, fileNames chan string, errors chan error) {
	defer dirsWG.Done()
	select {
	case <-stop:
		fmt.Println("ReadDir, stop", dirName)
		stop <- struct{}{}
		return
	default:
		fileInfos, err := ioutil.ReadDir(dirName)
		if err != nil {
			errors <- err
			return
		}

		for _, fi := range fileInfos {
			if fi.IsDir() {
				dirsWG.Add(1)
				go ReadDir(dirName+string(os.PathSeparator)+fi.Name(), fileNames, errors)
			} else {
				location := dirName + string(os.PathSeparator) + fi.Name()
				select {
				case <-stop:
					fmt.Println("ReadDir, stop", dirName)
					stop <- struct{}{}
				default:
					fileNames <- location
				}
			}
		}
		return
	}
}

func RunFiles(fileNames chan string, errors chan error, dirsDone chan struct{}, filesDone chan struct{}) {
	wg := sync.WaitGroup{}
Loop:
	for true {
		select {
		case <-stop:
			fmt.Println("RunFiles, stop")
			stop <- struct{}{}
			return
		case location := <-fileNames:
			wg.Add(1)
			go HashFile(location, &wg, errors)
		case <-dirsDone:
			break Loop
		default:
			runtime.Gosched()
		}
	}
	wg.Wait()
	filesDone <- struct{}{}
}

func HashFile(location string, wg *sync.WaitGroup, errors chan error) {
	defer wg.Done()
	f, err := os.Open(location)
	if err != nil {
		errors <- err
		return
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		errors <- err
		return
	}
	key := fmt.Sprintf("%x", h.Sum(nil))
	//fmt.Println(key, location)
	if fileHashes.Has(key) {
		if duplicates.Has(key) {
			duplicates.Append(key, location)
		} else {
			oldLocation, _ := fileHashes.Get(key)
			duplicates.Add(key, []string{oldLocation, location})
		}
	} else {
		fileHashes.Add(key, location)
	}
}
