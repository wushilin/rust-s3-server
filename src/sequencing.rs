use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct Sequence {
    seq:AtomicU64
}

impl Sequence {
    pub fn next(&self) -> String {
        let start = std::time::SystemTime::now();
        let time_part = start
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        return format!("{}_{}", time_part, self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
    }
}
impl Default for Sequence {
    fn default() -> Self {
        return Sequence {
            seq: AtomicU64::new(0),
        }
    }
}