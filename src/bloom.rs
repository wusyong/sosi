use crate::hash::hash32;

pub struct BloomFilterPolicy {
    hash_fn_num: u8,
    bits_per_key: usize,
}

fn bloom_hash(key: &[u8]) -> u32 {
    hash32(key, key.len(), 0xbc9f1d34)
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let mut hash_fn_num = bits_per_key as f32 * 0.69;
        if hash_fn_num < 1.0 {
            hash_fn_num = 1.0;
        }
        if hash_fn_num > 30.0 {
            hash_fn_num = 30.0;
        }

        BloomFilterPolicy {
            hash_fn_num: hash_fn_num as u8,
            bits_per_key,
        }
    }
    pub fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        // Calculate bloom filter size
        let mut bits: usize = keys.len() * self.bits_per_key;

        if bits < 64 {
            bits = 64;
        }

        let bytes: usize = (bits + 7) / 8;
        bits = bytes * 8;


        // initialize filter
        let mut filter = vec![0u8; bytes + 1];
        filter[bytes] = self.hash_fn_num;

        for key in keys {
            let mut h = bloom_hash(key);
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.hash_fn_num {
                let bitpos = h as usize % bits;
                filter[bitpos / 8] |= 1 << (bitpos % 8);
                h = h.wrapping_add(delta);
            }
        }

        filter
    }
    pub fn key_may_match(&self, key: &[u8], bloom_filter: &[u8]) -> bool {
        let len: usize = bloom_filter.len();
        if len < 2 {
            return false;
        }

        let filter = &bloom_filter[0..len - 1];
        let bits: usize = (len - 1) * 8;

        let hash_fn_num: usize = bloom_filter[len - 1] as usize;
        if hash_fn_num > 30 {
            // this might be a new encoding for bloom filter
            return true;
        }

        let mut h: u32 = bloom_hash(key);
        let delta: u32 = (h >> 17) | (h << 15);
        for _j in 0..hash_fn_num {
            let bitpos = h as usize % bits;
            if (filter[bitpos / 8] & (1 << (bitpos % 8))) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        return true;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_usage() {
        let policy: BloomFilterPolicy = BloomFilterPolicy::new(10);
        let keys = vec![String::from("hello").into_bytes(), String::from("world").into_bytes()];

        let bloom_filter = policy.create_filter(&keys);
        assert_eq!(policy.key_may_match("hello".as_bytes(), &bloom_filter), true);
        assert_eq!(policy.key_may_match("world".as_bytes(), &bloom_filter), true);
        assert_eq!(policy.key_may_match("helllo".as_bytes(), &bloom_filter), false);
        assert_eq!(policy.key_may_match("".as_bytes(), &bloom_filter), false);
        assert_eq!(policy.key_may_match("foo".as_bytes(), &bloom_filter), false);
    }

    #[test]
    fn empty_filter() {
        let policy: BloomFilterPolicy = BloomFilterPolicy::new(10);

        let bloom_filter = policy.create_filter(&Vec::new());
        assert_eq!(policy.key_may_match("hello".as_bytes(), &bloom_filter), false);
        assert_eq!(policy.key_may_match("world".as_bytes(), &bloom_filter), false);
    }

    #[test]
    fn varying_length() {
        let policy: BloomFilterPolicy = BloomFilterPolicy::new(10);
        let mut bloom_filter = Vec::new();
        let mut keys = Vec::new();
        let mut mediocre_filters: u32 = 0;
        let mut good_filters: u32 = 0;

        fn reset(keys: &mut Vec<Vec<u8>>, bloom_filter: &mut Vec<u8>) {
            keys.clear();
            bloom_filter.clear();
        }

        fn gen_key(i: u32) -> [u8; 4] {
            i.to_le_bytes()
        }

        fn next_length(length: u32) -> u32 {
            let mut len: u32 = length;
            if length < 10 {
                len += 1
            } else if length < 100 {
                len += 10
            } else if length < 1000 {
                len += 100
            } else {
                len += 1000
            }
            return len;
        }

        fn false_positive_rate(policy: &BloomFilterPolicy, bloom_filter: &[u8]) -> f64 {
            let mut result: f64 = 0.0;

            for i in 0..10000 {
                if policy.key_may_match(&gen_key(i + 1000000000), bloom_filter)
                {
                    result += 1.0;
                }
            }
            return result / 10000.0;
        }

        let mut length: u32 = 1;
        while length < 10000 {
            reset(&mut keys, &mut bloom_filter);

            for i in 0..length {
                let key = gen_key(i);
                keys.push(key.to_vec());
            }

            let bloom_filter = policy.create_filter(&keys);
            assert_eq!(bloom_filter.len() <= (length as usize * 10 / 8) + 40, true);

            // all keys added in filter must match
            for i in 0..length {
                assert_eq!(
                    policy.key_may_match(&gen_key(i), &bloom_filter),
                    true
                );
            }

            // false positive rate must lower than 0.02
            let rate: f64 = false_positive_rate(&policy, &bloom_filter);
            assert!(rate <= 0.02);

            if rate > 0.0125 {
                mediocre_filters += 1;
            } else {
                good_filters += 1;
            }

            length = next_length(length);
        }

        assert_eq!(mediocre_filters <= good_filters / 5, true);
    }
}
