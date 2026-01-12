use std::io::{stdin, stdout, BufReader, BufWriter, Write, BufRead};

const MAX_COLUMNS: usize = 1 << 16;

fn main() {
    // setup io
    let mut reader = BufReader::with_capacity(1024 * 512, stdin());
    let mut writer = BufWriter::with_capacity(1024 * 512, stdout());
    let mut buffer: Vec<u8> = Vec::new();
    // setup state
    let mut offsets: [usize; MAX_COLUMNS] = [0; MAX_COLUMNS];
    let mut lens:    [usize; MAX_COLUMNS] = [0; MAX_COLUMNS];
    // process input row by row
    loop {
        // read the next row into the buffer
        buffer.clear();
        match reader.read_until(b'\n', &mut buffer) {
            Err(err) => std::panic!(err),
            Ok(0) => break,
            // process the current row
            Ok(mut n) => {
                if buffer[n - 1] == b'\n' {
                    n -= 1;
                }
                if n > 0 {
                    // parse row
                    let mut offset = 0;
                    for (i, part) in buffer[..n].split(|val| val == &b',').enumerate() {
                        offsets[i] = offset;
                        lens[i] = part.len();
                        offset += part.len() + 1;
                    }
                    // handle row
                    writer.write_all(&buffer[..n][offsets[2]..offsets[2]+lens[2]]).unwrap();
                    writer.write_all(&[b',']).unwrap();
                    writer.write_all(&buffer[..n][offsets[6]..offsets[6]+lens[6]]).unwrap();
                    writer.write_all(&[b'\n']).unwrap();
                }
            }
        }
    }
}
