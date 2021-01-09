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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_32_bits() {
        let data1: [u8; 1] = [0x62];
        let data2: [u8; 2] = [0xc3, 0x97];
        let data3: [u8; 3] = [0xe2, 0x99, 0xa5];
        let data4: [u8; 4] = [0xe1, 0x80, 0xb9, 0x32];
        let data5: [u8; 48] = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(hash32(&[], 0, 0xbc9f1d34), 0xbc9f1d34);
        assert_eq!(hash32(&data1, 1, 0xbc9f1d34), 0xef1345c4);
        assert_eq!(hash32(&data2, 2, 0xbc9f1d34), 0x5b663814);
        assert_eq!(hash32(&data3, 3, 0xbc9f1d34), 0x323c078f);
        assert_eq!(hash32(&data4, 4, 0xbc9f1d34), 0xed21633a);
        assert_eq!(hash32(&data5, 48, 0x12345678), 0xf333dabb);
    }
}
