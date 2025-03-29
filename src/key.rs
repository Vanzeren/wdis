
use byteorder::{LittleEndian, WriteBytesExt};

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}
trait VarintExt {
    fn extend_varint(&mut self, num: usize);
}

impl VarintExt for Vec<u8> {
    fn extend_varint(&mut self, mut num: usize) {
        loop {
            let byte = (num & 0x7F) as u8;
            num >>= 7;
            if num == 0 {
                self.push(byte);
                break;
            } else {
                self.push(byte | 0x80);
            }
        }
    }
}

pub fn build_mem_key(seq: u64, t: ValueType, key: &[u8]) -> Vec<u8> {
    let keysize = key.len() + 8; // Key + Seq/Type（固定 8 字节）

    // 预分配精确容量
    let mut buf = Vec::with_capacity(varint_len(keysize) + keysize);

    // 写入 Key 部分
    buf.extend_varint(keysize);
    buf.extend_from_slice(key);
    buf.write_u64::<LittleEndian>((t as u64) | (seq << 8)).unwrap();
    buf
}

pub fn build_mem_value(value: &[u8]) -> Vec<u8> {
    let valuesize = value.len(); 

    // 预分配精确容量
    let mut buf = Vec::with_capacity(varint_len(valuesize) + valuesize);

    // 写入 Key 部分
    buf.extend_varint(valuesize);
    buf.extend_from_slice(value);

    buf
}

fn varint_len(mut num: usize) -> usize {
    let mut len = 0;
    loop {
        len += 1;
        num >>= 7;
        if num == 0 {
            break;
        }
    }
    len
}


#[cfg(test)]
mod test {
    use crate::key::build_mem_value;

    use super::build_mem_key;
    use super::ValueType;

    
    #[test]
    fn test_build_memtable() {
        assert_eq!(
            build_mem_key(
                231,
                ValueType::TypeValue,
                "abc".as_bytes(),
            )
            ,
            vec![11, 97, 98, 99, 1, 231, 0, 0, 0, 0, 0, 0]
        );

        println!("{:?}",build_mem_value("123".as_bytes()));
    }
}


