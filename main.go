package main

import (
	"crypto/md5"
	"find-duplicates/set"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

var (
	path        *string
	fileHashes  = set.NewStringStringSet()
	duplicates  = set.NewStringSliceSet()
	dirsWG      = new(sync.WaitGroup)
	stop        = make(chan struct{}, 1)
	filesBuffer = 100
	logLevel    *uint
	logger      *log.Logger
)

func init() {
	logLevelUsage := "Logging level:"
	for _, level := range log.AllLevels {
		logLevelUsage = logLevelUsage + fmt.Sprintf("\n\t%d - %s", level, level.String())
	}
	path = flag.String("path", "data", "path to the directory where duplicates will be searched")
	logLevel = flag.Uint("log_level", 1, logLevelUsage)
	flag.Parse()
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.Level(*logLevel))
	logger = log.StandardLogger()
}

func main() {
	fields := log.Fields{
		"function": "main",
	}
	defer recoverPanic(fields)
	fileNames := make(chan string, filesBuffer)
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
		fields := log.Fields{
			"function": "main@anonymous",
		}
		select {
		case err := <-errors:
			if err != nil {
				logger.WithFields(fields).Errorf("error: %+v", err)
				stop <- struct{}{}
			}
		case <-filesDone:
			logger.WithFields(fields).Debug("all files have been processed")
			filesDone <- struct{}{}
		case <-stop:
			logger.WithFields(fields).Debug("stop signal received")
			stop <- struct{}{}
		}
	}()
	dirsWG.Wait()
	time.Sleep(time.Second * 2)
	logger.WithFields(fields).Debug("dirs done")
	dirsDone <- struct{}{}
	select {
	case <-stop:
		logger.WithFields(fields).Debug("stop signal received")
		stop <- struct{}{}
		return
	case <-filesDone:
		logger.WithFields(fields).Debug("all files have been processed")
	}
	duplicates.Print()
}

func ReadDir(dirName string, fileNames chan string, errors chan error) {
	defer dirsWG.Done()
	fields := log.Fields{
		"function": "ReadDir",
		"path":     dirName,
	}
	defer recoverPanic(fields)
	select {
	case <-stop:
		logger.WithFields(fields).Debug("stop signal received")
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
					logger.WithFields(fields).Debug("stop signal received")
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
	fields := log.Fields{
		"function": "RunFiles",
	}
	defer recoverPanic(fields)
Loop:
	for true {
		select {
		case <-stop:
			logger.WithFields(fields).Debug("stop signal received")
			stop <- struct{}{}
			return
		case location := <-fileNames:
			logger.WithFields(fields).WithField("file", location).Info("file has been submitted for processing")
			wg.Add(1)
			go HashFile(location, &wg, errors)
		case <-dirsDone:
			logger.WithFields(fields).Debug("all files have been submitted for processing")
			break Loop
		}
	}
	wg.Wait()
	logger.WithFields(fields).Debug("all files have been processed")
	filesDone <- struct{}{}
}

func HashFile(location string, wg *sync.WaitGroup, errors chan error) {
	fields := log.Fields{
		"path":     location,
		"function": "HashFile",
	}
	defer func() {
		wg.Done()
		logger.WithFields(fields).Info("finished parsing the file")
	}()
	defer recoverPanic(fields)
	if location == "/home/eol/data/inner_folder_1/inner_folder_1/4.txt" {
		panic("something went wrong")
	}
	f, err := os.Open(location)
	logger.WithFields(fields).Info("started parsing the file")
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
		logger.WithFields(fields).Debug("the file is a duplicate")
		if duplicates.Has(key) {
			duplicates.Append(key, location)
		} else {
			oldLocation, _ := fileHashes.Get(key)
			duplicates.Add(key, []string{oldLocation, location})
		}
	} else {
		logger.WithFields(fields).Debug("the file is currently unique")
		fileHashes.Add(key, location)
	}
}

func recoverPanic(fields log.Fields) {
	if r := recover(); r != nil {
		logger.WithFields(fields).WithField("stack", string(debug.Stack())).Fatal("there was a panic")
		stop <- struct{}{}
	}
}
