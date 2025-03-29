use crate::key::{self, ValueType};
use skl::{
    generic::{
        unique::{sync::SkipMap, Map},
        Builder,
    },
    Arena,
};

pub struct MemTable {
    map: SkipMap<Vec<u8>, Vec<u8>>,
}


impl MemTable {
    pub fn new() -> MemTable {
        let l = Builder::new()
            .with_capacity(4 << 20)
            .alloc::<SkipMap<Vec<u8>, Vec<u8>>>()
            .unwrap();
        MemTable { map: l }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn allocated(&self) -> usize {
        self.map.allocated()
    }

    pub fn add(&self, seq: u64, t: ValueType, user_key: &[u8], value: &[u8]) {
        let memkey = key::build_mem_key(seq, t, user_key);
        let memval = key::build_mem_value(value);
        self.map.insert(&memkey, &memval).unwrap();
    }

    pub fn get(&self,user_key: &[u8],seq: u64) -> Option<Vec<u8>>{
        let find_key = key::build_mem_key(seq, ValueType::TypeValue, user_key);
        let find =  match self.map.get(&find_key) {
            Some(ent) => ent,
            None => return None
        };
        Some(find.value().to_vec())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    
    #[test]
    fn test_memtable_basic() {
        let memtable = MemTable::new();
        assert_eq!(memtable.len(), 0);
        // Test add and get
        memtable.add(1, ValueType::TypeValue, b"key1", b"value1");
        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(b"key1", 1), Some(b"value1".to_vec()));
        
        // Test non-existent key
        assert_eq!(memtable.get(b"key2", 1), None);
    }

    #[test]
    fn test_memtable_concurrent() {
        let memtable = Arc::new(MemTable::new());
        let mut handles = vec![];
        
        // Spawn 10 threads to concurrently insert
        for i in 0..10 {
            let memtable = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{}_{}", i, j);
                    let value = format!("value_{}_{}", i, j);
                    memtable.add((i * 100 + j) as u64, ValueType::TypeValue, key.as_bytes(), value.as_bytes());
                }
            }));
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify all entries were inserted
        assert_eq!(memtable.len(), 1000);
        
        // Verify some random entries
        assert!(memtable.get(b"key_5_50", 550).is_some());
        assert!(memtable.get(b"key_9_99", 999).is_some());
    }

    #[test]
    fn test_memtable_allocated() {
        let memtable = MemTable::new();
        let initial = memtable.allocated();
        
        memtable.add(1, ValueType::TypeValue, b"key1", b"value1");
        assert!(memtable.allocated() > initial);
    }
}
