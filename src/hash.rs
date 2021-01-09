pub fn hash32(data: &[u8], n: usize, seed: u32) -> u32 {
    let m: u32 = 0xc6a4a793;
    let r: u32 = 24;
    let mut h: u32 = seed ^ ((n as u32).wrapping_mul(m));

    let mut pos: usize = 0;
    while pos + 4 <= n {
        let mut w = [0u8; 4];
        w.copy_from_slice(&data[pos..pos + 4]);

        h = h.wrapping_add(u32::from_le_bytes(w));
        h = h.wrapping_mul(m);
        h ^= h >> 16;

        pos += 4;
    }

    if let Some(b) = remain_bytes(data, n, pos) {
        h = h.wrapping_add(b);
        h = h.wrapping_mul(m);
        h ^= h >> r;
    }

    h
}

fn remain_bytes(data: &[u8], n: usize, curr_pos: usize) -> Option<u32> {
    match n - curr_pos {
        3 => Some(
            (data[curr_pos + 2] as u32) << 16
                | (data[curr_pos + 1] as u32) << 8
                | data[curr_pos] as u32,
        ),
        2 => Some((data[curr_pos + 1] as u32) << 8 | data[curr_pos] as u32),
        1 => Some(data[curr_pos] as u32),
        _ => None,
    }
}
