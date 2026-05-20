use std::time::Instant;

fn zero_copy_decode(data: &[u8]) -> &[u64] {
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u64, data.len() / 8) }
}

fn vec_decode(data: &[u8]) -> Vec<u64> {
    data.chunks_exact(8)
        .map(|c| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(c);
            u64::from_le_bytes(bytes)
        })
        .collect()
}

fn main() {
    let sizes = [100, 10_000, 1_000_000];
    for &n in &sizes {
        let data: Vec<u8> = (0..n).flat_map(|i: u64| i.to_le_bytes()).collect();
        let bytes = data.len();
        let iters = if n <= 10_000 { 1000 } else { 100 };

        let start = Instant::now();
        for _ in 0..iters {
            let slice = zero_copy_decode(&data);
            std::hint::black_box(slice.iter().sum::<u64>());
        }
        let zc_time = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            let vec = vec_decode(&data);
            std::hint::black_box(vec.iter().sum::<u64>());
        }
        let vec_time = start.elapsed();

        println!(
            "n={n:>9} ({bytes:>8}B)  zero-copy: {zc_time:>8?}  vec: {vec_time:>8?}  speedup: {:.1}x",
            vec_time.as_nanos() as f64 / zc_time.as_nanos() as f64
        );
    }
}
