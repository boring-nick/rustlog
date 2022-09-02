use std::{
    io::{self, Cursor, Read, Write},
    mem::size_of,
};

pub const SIZE: usize = size_of::<Index>();

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Index {
    pub day: u32,
    pub offset: u64,
    pub len: u32,
}

impl Index {
    pub fn as_bytes(&self) -> [u8; SIZE] {
        let result = [0u8; SIZE];
        let mut cursor = Cursor::new(result);

        cursor.write_all(&self.day.to_be_bytes()).unwrap();
        cursor.write_all(&self.offset.to_be_bytes()).unwrap();
        cursor.write_all(&self.len.to_be_bytes()).unwrap();

        cursor.into_inner()
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut day_bytes = [0u8; size_of::<u32>()];
        cursor.read_exact(&mut day_bytes)?;

        let mut offset_bytes = [0u8; size_of::<u64>()];
        cursor.read_exact(&mut offset_bytes)?;

        let mut len_bytes = [0u8; size_of::<u32>()];
        cursor.read_exact(&mut len_bytes)?;

        Ok(Self {
            day: u32::from_be_bytes(day_bytes),
            offset: u64::from_be_bytes(offset_bytes),
            len: u32::from_be_bytes(len_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Index, SIZE};

    #[test]
    fn index_type_size() {
        assert_eq!(SIZE, 16);
    }

    #[test]
    fn index_as_bytes() {
        let index = Index {
            day: 12,
            offset: 34213,
            len: 40,
        };
        let bytes = index.as_bytes();
        assert_eq!(
            bytes,
            [0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 133, 165, 0, 0, 0, 40]
        )
    }

    #[test]
    fn index_from_bytes() {
        let bytes = vec![0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 133, 165, 0, 0, 0, 40];
        let index = Index::from_bytes(&bytes).unwrap();
        assert_eq!(
            index,
            Index {
                day: 12,
                offset: 34213,
                len: 40
            }
        )
    }
}
