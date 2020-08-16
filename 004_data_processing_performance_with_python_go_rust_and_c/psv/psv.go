package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"psv/row"

	"github.com/golang/protobuf/proto"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	f := bufio.NewWriter(os.Stdout)
	defer f.Flush()
	for scanner.Scan() {
		line := scanner.Text()
		line_bytes := []byte(line)
		parts := bytes.Split(line_bytes, []byte{','})
		r := &row.Row{
			Column0: parts[0],
			Column1: parts[1],
			Column2: parts[2],
			Column3: parts[3],
			Column4: parts[4],
			Column5: parts[5],
			Column6: parts[6],
			Column7: parts[7],
		}
		res, err := proto.Marshal(r)
		if err != nil {
			panic(err)
		}
		err = binary.Write(f, binary.LittleEndian, int32(len(res)))
		if err != nil {
			panic(err)
		}
		_, err = f.Write(res)
		if err != nil {
			panic(err)
		}
	}
}
