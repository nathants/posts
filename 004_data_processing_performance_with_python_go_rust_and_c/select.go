package main

import (
	"bufio"
	"io"
	"os"
)

func main() {
	starts := make([]int, 1<<16)
	ends := make([]int, 1<<16)
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for {
		// read row
		row, isPrefix, err := r.ReadLine()
		if isPrefix {
			panic("row too long")
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			panic(err)
		}
		// parse row
		offset := 0
		max := 0
		for i := 0; i < len(row); i++ {
			switch row[i] {
			case byte(','):
				starts[max] = offset
				ends[max] = i
				offset = i + 1
				max += 1
			}
		}
		starts[max] = offset
		ends[max] = len(row)
		// handle row
		_, err = w.Write(row[starts[2]:ends[2]])
		if err != nil {
			panic(err)
		}
		_, err = w.Write([]byte(","))
		if err != nil {
			panic(err)
		}
		_, err = w.Write(row[starts[6]:ends[6]])
		if err != nil {
			panic(err)
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}
	}
}
