//! Port of `farm.Fingerprint32` from github.com/dgryski/go-farm (farmhashmk `Hash32`).
//!
//! Temporal 1.31.0 uses this hash in two places that matter for hotspot modelling:
//! * `common.WorkflowIDToHistoryShard`: `Fingerprint32(namespaceID + "_" + workflowID) % numShards + 1`
//! * ringpop's consistent hash ring (shard -> history host, task queue partition -> matching host).
//!
//! Porting it exactly means shard placement and ring imbalance follow the same statistics as a
//! real cluster (and match a real cluster exactly when real member addresses are supplied).

const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

#[inline]
fn fetch32(s: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([s[i], s[i + 1], s[i + 2], s[i + 3]])
}

#[inline]
fn fmix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

#[inline]
fn mur(mut a: u32, mut h: u32) -> u32 {
    a = a.wrapping_mul(C1);
    a = a.rotate_right(17);
    a = a.wrapping_mul(C2);
    h ^= a;
    h = h.rotate_right(19);
    h.wrapping_mul(5).wrapping_add(0xe654_6b64)
}

fn hash32_len_0_to_4(s: &[u8], seed: u32) -> u32 {
    let mut b = seed;
    let mut c: u32 = 9;
    for &byte in s {
        // go-farm uses int8(s[i]) i.e. a signed char, sign-extended to uint32.
        let v = i32::from(byte as i8) as u32;
        b = b.wrapping_mul(C1).wrapping_add(v);
        c ^= b;
    }
    fmix(mur(b, mur(s.len() as u32, c)))
}

fn hash32_len_5_to_12(s: &[u8], seed: u32) -> u32 {
    let slen = s.len();
    let mut a = slen as u32;
    let mut b = (slen as u32).wrapping_mul(5);
    let mut c: u32 = 9;
    let d = b.wrapping_add(seed);
    a = a.wrapping_add(fetch32(s, 0));
    b = b.wrapping_add(fetch32(s, slen - 4));
    c = c.wrapping_add(fetch32(s, (slen >> 1) & 4));
    fmix(seed ^ mur(c, mur(b, mur(a, d))))
}

fn hash32_len_13_to_24(s: &[u8], seed: u32) -> u32 {
    let slen = s.len();
    let mut a = fetch32(s, (slen >> 1) - 4);
    let b = fetch32(s, 4);
    let c = fetch32(s, slen - 8);
    let d = fetch32(s, slen >> 1);
    let e = fetch32(s, 0);
    let f = fetch32(s, slen - 4);
    let mut h = d
        .wrapping_mul(C1)
        .wrapping_add(slen as u32)
        .wrapping_add(seed);
    a = a.rotate_right(12).wrapping_add(f);
    h = mur(c, h).wrapping_add(a);
    a = a.rotate_right(3).wrapping_add(c);
    h = mur(e, h).wrapping_add(a);
    a = a.wrapping_add(f).rotate_right(12).wrapping_add(d);
    h = mur(b ^ seed, h).wrapping_add(a);
    fmix(h)
}

/// farmhash `Fingerprint32` (== go-farm `Hash32`).
pub fn fingerprint32(s: &[u8]) -> u32 {
    let slen = s.len();
    if slen <= 24 {
        if slen <= 12 {
            if slen <= 4 {
                return hash32_len_0_to_4(s, 0);
            }
            return hash32_len_5_to_12(s, 0);
        }
        return hash32_len_13_to_24(s, 0);
    }

    let mut h = slen as u32;
    let mut g = C1.wrapping_mul(slen as u32);
    let mut f = g;
    let mix = |x: u32| x.wrapping_mul(C1).rotate_right(17).wrapping_mul(C2);
    let a0 = mix(fetch32(s, slen - 4));
    let a1 = mix(fetch32(s, slen - 8));
    let a2 = mix(fetch32(s, slen - 16));
    let a3 = mix(fetch32(s, slen - 12));
    let a4 = mix(fetch32(s, slen - 20));
    h ^= a0;
    h = h.rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    h ^= a2;
    h = h.rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    g ^= a1;
    g = g.rotate_right(19);
    g = g.wrapping_mul(5).wrapping_add(0xe654_6b64);
    g ^= a3;
    g = g.rotate_right(19);
    g = g.wrapping_mul(5).wrapping_add(0xe654_6b64);
    f = f.wrapping_add(a4);
    f = f.rotate_right(19).wrapping_add(113);

    let mut rest = s;
    while rest.len() > 20 {
        let a = fetch32(rest, 0);
        let b = fetch32(rest, 4);
        let c = fetch32(rest, 8);
        let d = fetch32(rest, 12);
        let e = fetch32(rest, 16);
        h = h.wrapping_add(a);
        g = g.wrapping_add(b);
        f = f.wrapping_add(c);
        h = mur(d, h).wrapping_add(e);
        g = mur(c, g).wrapping_add(a);
        f = mur(b.wrapping_add(e.wrapping_mul(C1)), f).wrapping_add(d);
        f = f.wrapping_add(g);
        g = g.wrapping_add(f);
        rest = &rest[20..];
    }
    g = g.rotate_right(11).wrapping_mul(C1);
    g = g.rotate_right(17).wrapping_mul(C1);
    f = f.rotate_right(11).wrapping_mul(C1);
    f = f.rotate_right(17).wrapping_mul(C1);
    h = h.wrapping_add(g).rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    h = h.rotate_right(17).wrapping_mul(C1);
    h = h.wrapping_add(f).rotate_right(19);
    h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    h = h.rotate_right(17).wrapping_mul(C1);
    h
}

/// `common.WorkflowIDToHistoryShard` from Temporal 1.31.0 (shard IDs start at 1).
pub fn workflow_id_to_history_shard(namespace_id: &str, workflow_id: &str, num_shards: u32) -> u32 {
    let mut key = Vec::with_capacity(namespace_id.len() + 1 + workflow_id.len());
    key.extend_from_slice(namespace_id.as_bytes());
    key.push(b'_');
    key.extend_from_slice(workflow_id.as_bytes());
    fingerprint32(&key) % num_shards + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test vectors from go-farm farm_test.go (oh32 column).
    const VECTORS: &[(u32, &str)] = &[
        (0xdc56d17a, ""),
        (0x3c973d4d, "a"),
        (0x417330fd, "ab"),
        (0x2f635ec7, "abc"),
        (0x98b51e95, "abcd"),
        (0xa3f366ac, "abcde"),
        (0x0f813aa4, "abcdef"),
        (0x21deb6d7, "abcdefg"),
        (0xfd7ec8b9, "abcdefgh"),
        (0x6f98dc86, "abcdefghi"),
        (0x9741ca1a, "0123456789"),
        (0xca179ba9, "0123456789 "),
        (0xf8cc7928, "0123456789-0"),
        (0x0d92cafb, "0123456789~01"),
        (0x71a36842, "0123456789#012"),
        (0x93ee6801, "0123456789@0123"),
        (0x9cecd750, "0123456789'01234"),
        (0x335f081f, "0123456789=012345"),
        (0xa9785062, "0123456789+0123456"),
        (0x5d4bd7f6, "0123456789*01234567"),
        (0x3884aa05, "0123456789&012345678"),
        (0x536d1efd, "0123456789^0123456789"),
        (0x1723dd7a, "0123456789%0123456789£"),
        (0xfa88d020, "0123456789$0123456789!0"),
        (0xc6246b8d, "size:  a.out:  bad magic"),
        (0x322984d9, "Nepal premier won't resign."),
        (0x221694e4, "C is as portable as Stonehedge!!"),
        (0xe273108f, "Discard medicine more than two years old."),
        (0x363394d1, "I wouldn't marry him with a ten foot pole."),
    ];

    #[test]
    fn matches_go_farm_vectors() {
        for &(want, input) in VECTORS {
            assert_eq!(fingerprint32(input.as_bytes()), want, "input {input:?}");
        }
    }

    #[test]
    fn shard_ids_are_one_based_and_in_range() {
        for i in 0..1000 {
            let s = workflow_id_to_history_shard("ns-id", &format!("wf-{i}"), 16);
            assert!((1..=16).contains(&s));
        }
    }
}
