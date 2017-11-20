package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func generateRandomString(length int) string {
	const symbols = "abcdef ghijklmnopqr stuvwxyz"
	result := make([]byte, length+1)
	for i := 0; i < length; i++ {
		result[i] = symbols[rand.Int31n(int32(len(symbols)))]
	}
	result[length] = '\n'
	return string(result)
}

func main() {

	var (
		rowsCount int64
		rowLength int
		fileName  string
	)

	flag.Int64Var(&rowsCount, "rows", 32*1024*1024, "rows count")
	flag.IntVar(&rowLength, "length", 80, "row length")
	flag.StringVar(&fileName, "file", "in.tmp", "file name")
	flag.Parse()

	rand.Seed(int64(time.Now().Nanosecond()))

	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		fmt.Errorf("can't open file: %s (%s)", fileName, err)
		os.Exit(1)
	}
	defer f.Close()

	for i := int64(0); i < rowsCount; i++ {
		res := generateRandomString(rowLength)
		_, err := f.WriteString(res)
		if err != nil {
			fmt.Errorf("write failed: %s", err)
			os.Exit(1)
		}
	}
}
