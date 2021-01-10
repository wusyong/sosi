use crate::hash::hash32;

pub struct BloomFilterPolicy {
    hash_fn_num: usize,
    bits_per_key: usize,
}

fn bloom_hash(key: &[u8]) -> u32 {
    hash32(key, key.len(), 0xbc9f1d34)
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let mut hash_fn_num: f32 = bits_per_key as f32 * 0.69;
        if hash_fn_num < 1.0 {
            hash_fn_num = 1.0;
        }
        if hash_fn_num > 30.0 {
            hash_fn_num = 30.0;
        }

        BloomFilterPolicy {
            hash_fn_num: hash_fn_num as usize,
            bits_per_key,
        }
    }
    pub fn create_filter(&self, keys: &Vec<String>, key_size: usize, filter: &Vec<u32>) -> Vec<u32> {
        let mut result: Vec<u32> = filter.clone();
        // Calculate bloom filter size
        let mut bits: usize = key_size * self.bits_per_key as usize;

        if bits < 64 {
            bits = 64;
        }

        let bytes: usize = (bits + 7) / 8;
        bits = bytes * 8;

        let init_size: usize = result.len();

        // initialize filter
        result.reserve(bytes);
        for _i in init_size..(init_size + bytes) {
            result.push(0);
        }
        result.push(self.hash_fn_num as u32);
        let filter: &mut [u32] = &mut result[init_size..];

        for i in 0..key_size {
            let mut h: u32 = bloom_hash(keys[i].as_bytes());
            let delta: u32 = (h >> 17) | (h << 15);
            for _j in 0..self.hash_fn_num {
                let bitpos: usize = h as usize % bits;
                filter[(bitpos / 8) as usize] |= 1 << (bitpos % 8);
                h = h.wrapping_add(delta);
            }
        }

        result
    }
    pub fn key_may_match(&self, key: &str, bloom_filter: &Vec<u32>) -> bool {
        let len: usize = bloom_filter.len();
        if len < 2 {
            return false;
        }

        let filter: &[u32] = &bloom_filter[0..len - 1];
        let bits: usize = (len - 1) * 8;

        let hash_fn_num: usize = bloom_filter[len - 1] as usize;
        if hash_fn_num > 30 {
            // this might be a new encoding for bloom filter
            return true;
        }

        let mut h: u32 = bloom_hash(key.as_bytes());
        let delta: u32 = (h >> 17) | (h << 15);
        for _j in 0..hash_fn_num {
            let bitpos: usize = h as usize % bits;
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
        let mut bloom_filter: Vec<u32> = Vec::new();
        let keys: Vec<String> = vec![String::from("hello"), String::from("world")];

        bloom_filter = policy.create_filter(&keys, 2, &bloom_filter);
        assert_eq!(policy.key_may_match("hello", &bloom_filter), true);
        assert_eq!(policy.key_may_match("world", &bloom_filter), true);
        assert_eq!(policy.key_may_match("helllo", &bloom_filter), false);
        assert_eq!(policy.key_may_match("", &bloom_filter), false);
        assert_eq!(policy.key_may_match("foo", &bloom_filter), false);
    }

    #[test]
    fn empty_filter() {
        let policy: BloomFilterPolicy = BloomFilterPolicy::new(10);
        let mut bloom_filter: Vec<u32> = Vec::new();

        bloom_filter = policy.create_filter(&Vec::new(), 0, &bloom_filter);
        assert_eq!(policy.key_may_match("hello", &bloom_filter), false);
        assert_eq!(policy.key_may_match("world", &bloom_filter), false);
    }

    #[test]
    fn varying_length() {
        let policy: BloomFilterPolicy = BloomFilterPolicy::new(10);
        let mut bloom_filter: Vec<u32> = Vec::new();
        let mut keys: Vec<String> = Vec::new();
        let mut mediocre_filters: u32 = 0;
        let mut good_filters: u32 = 0;

        fn reset(keys: &mut Vec<String>, bloom_filter: &mut Vec<u32>) {
            keys.clear();
            bloom_filter.clear();
        }

        fn gen_key(i: u32) -> u32 {
            u32::from_le_bytes(i.to_be_bytes())
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

        fn false_positive_rate(policy: &BloomFilterPolicy, bloom_filter: &Vec<u32>) -> f64 {
            let mut result: f64 = 0.0;

            for i in 0..10000 {
                if policy.key_may_match(gen_key(i + 1000000000).to_string().as_str(), bloom_filter)
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
                let key: String = gen_key(i).to_string();
                keys.push(key);
            }

            bloom_filter = policy.create_filter(&keys, keys.len(), &bloom_filter);
            assert_eq!(bloom_filter.len() <= (length as usize * 10 / 8) + 40, true);

            // all keys added in filter must match
            for i in 0..length {
                assert_eq!(
                    policy.key_may_match(gen_key(i).to_string().as_str(), &bloom_filter),
                    true
                );
            }

            // false positive rate must lower than 0.02
            let rate: f64 = false_positive_rate(&policy, &bloom_filter);
            assert_eq!(rate <= 0.02, true);

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
