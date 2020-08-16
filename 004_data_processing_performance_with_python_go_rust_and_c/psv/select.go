package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"psv/row"

	"github.com/golang/protobuf/proto"
)

const max_size = 1024 * 1024 * 5

func main() {
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	buf := make([]byte, max_size)
	row := &row.Row{}
	for {
		var size int32
		err := binary.Read(r, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if size > max_size {
			panic("row too large")
		}
		n, err := io.ReadFull(r, buf[:size])
		if n != int(size) {
			panic(fmt.Sprintf("not size: %d %d", n, int(size)))
		}
		if err != nil {
			panic(err)
		}
		err = proto.Unmarshal(buf[:size], row)
		if err != nil {
			panic(err)
		}
		_, err = w.Write(row.Column2)
		if err != nil {
			panic(err)
		}
		_, err = w.Write([]byte(","))
		if err != nil {
			panic(err)
		}
		_, err = w.Write(row.Column6)
		if err != nil {
			panic(err)
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}
	}
}
