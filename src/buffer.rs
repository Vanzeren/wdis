pub struct buf {
    pub size: usize,
    pub data: Box<[u8]>,
}


impl buf {
    pub fn new(size: usize) -> buf {
        buf {
            size: size,
            data: vec![0; size].into_boxed_slice(),
        }
    }

    // pub fn parse(&self) -> cmd{
        
    // }
}
