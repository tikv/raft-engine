use std::io::{Result, Write};

pub struct LengthFixedIoVec {
    row_capacity: usize,
    buffers: Vec<Vec<u8>>,
}

impl LengthFixedIoVec {
    pub fn new(row_capacity: usize) -> LengthFixedIoVec {
        LengthFixedIoVec {
            row_capacity,
            buffers: vec![],
        }
    }
}

impl Write for LengthFixedIoVec {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.buffers.is_empty() {
            self.buffers.push(Vec::with_capacity(self.row_capacity));
        }
        let buffer = self.buffers.last_mut().unwrap();
        if buffer.len() + buf.len() <= self.row_capacity {
            buffer.extend_from_slice(buf);
            return Ok(buf.len());
        }
        let size = self.row_capacity - buffer.len();
        buffer.extend_from_slice(&buf[..size]);
        self.buffers.push(Vec::with_capacity(self.row_capacity));
        Ok(size)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
