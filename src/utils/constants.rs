pub const AWS_MAX_RETRIES: u32 = 10;
pub const CHUNK_SIZE: u64 = 10_000_000; // 10 MiB
pub const MAX_CHUNKS: u64 = 10_000; // 10 GiB
pub const CHUNKS_WORKERS: usize = 10; // max chunks upload in parallel
pub const CHUNKS_MAX_RETRY: u64 = 5; // max retry for chunk
