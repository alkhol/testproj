package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

func main() {
	var (
		inFileName, outFileName       string
		parallelCount, linesPerWorker int
	)

	flag.IntVar(&parallelCount, "parallel", 16, "workers count")
	flag.IntVar(&linesPerWorker, "max_lines", 512*1024, "max lines per worker")
	flag.StringVar(&inFileName, "ifile", "in.tmp", "input file name")
	flag.StringVar(&outFileName, "ofile", "out.tmp", "output file name")
	flag.Parse()

	sortCh := make(chan []string)
	mergeCh := make(chan string)
	waitMerge := make(chan struct{})
	var wg sync.WaitGroup
	var toMerge []string

	defer func() {
		for _, file := range toMerge {
			os.Remove(file)
		}
	}()

	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for toSort := range sortCh {
				if tmpFile, err := sortPart(toSort); err == nil {
					mergeCh <- tmpFile
				}
			}
		}()
	}

	go func() {
		for fileName := range mergeCh {
			toMerge = append(toMerge, fileName)
		}
		close(waitMerge)
	}()

	f, err := os.Open(inFileName)
	if err != nil {
		fmt.Printf("can't open file %s: %s\n", inFileName, err)
		os.Exit(1)
	}
	defer f.Close()
	reader := bufio.NewReader(f)

	var toSort []string
	for {
		toSort, err = readChunk(reader, linesPerWorker)
		if err != nil && err != io.EOF {
			fmt.Printf("error while reading: %s\n", err)
			os.Exit(1)
		}
		if len(toSort) == 0 {
			break
		}
		sortCh <- toSort
	}

	if len(toSort) > 0 {
		sortCh <- toSort
	}

	close(sortCh)
	wg.Wait()
	close(mergeCh)
	<-waitMerge

	if err := merge(toMerge, outFileName); err != nil {
		fmt.Printf("merge error: %s", err)
		os.Exit(1)
	}
}

func merge(inpNames []string, outName string) error {
	out, err := os.OpenFile(outName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer out.Close()

	readers := make([]*bufio.Reader, len(inpNames))
	chunks := make([][]string, len(inpNames))

	for i, fileName := range inpNames {
		f, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer f.Close()
		readers[i] = bufio.NewReader(f)
	}

	for {
		curMin := ""
		minChunkNum := -1
		for i := 0; i < len(readers); i++ {
			if len(chunks[i]) == 0 {
				chunks[i], err = readChunk(readers[i], 1024*256)
				if len(chunks[i]) == 0 {
					readers[i], readers[len(readers)-1] = readers[len(readers)-1], readers[i]
					chunks[i], chunks[len(chunks)-1] = chunks[len(chunks)-1], chunks[i]
					readers = readers[0 : len(readers)-1]
					chunks = chunks[0 : len(chunks)-1]
					i--
					continue
				}

				if err != nil && err != io.EOF {
					return err
				}
			}

			if chunks[i][0] < curMin || curMin == "" {
				curMin = chunks[i][0]
				minChunkNum = i
			}
		}

		if curMin == "" {
			return nil
		}

		_, err = out.WriteString(curMin)
		if err != nil {
			return err
		}

		chunks[minChunkNum] = chunks[minChunkNum][1:]
	}

	return nil
}

func sortPart(toSort []string) (string, error) {
	f, err := ioutil.TempFile(".", "part")
	if err != nil {
		return "", err
	}
	defer f.Close()

	sort.Strings(toSort)
	for _, str := range toSort {
		if _, err := f.WriteString(str); err != nil {
			return "", err
		}

	}
	return f.Name(), nil
}

func readChunk(reader *bufio.Reader, lines int) ([]string, error) {
	toRead := make([]string, 0, lines)
	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			return toRead, err
		}
		toRead = append(toRead, str)
		if len(toRead) == lines {
			return toRead, nil
		}
	}
}
