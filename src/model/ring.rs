//! Consistent hash ring identical to `temporalio/ringpop-go/hashring` as used by Temporal 1.31.0
//! (`common/membership/ringpop/service_resolver.go`).
//!
//! * Each member contributes `system.ringpopReplicaPoints` (default 100) points whose hash is
//!   `farm.Fingerprint32(address + index)` (the legacy format used when the member identity equals
//!   its address, which is the case for Temporal).
//! * Points are ordered by (hash, address, index).
//! * `lookup(key)` hashes the key and returns the owner of the first point with hash >= key hash,
//!   wrapping around; `lookup_n` collects distinct owners walking forward.
//!
//! History shards are looked up with the key `strconv.Itoa(shardID)`; matching partitions with
//! the routing key `"<namespaceID>:<rpcName>:<taskType>"`.

use crate::util::farmhash::fingerprint32;

#[derive(Clone, Debug)]
pub struct HashRing {
    /// (hash, address index into `members`, replica index), sorted.
    points: Vec<(u32, usize, u32)>,
    members: Vec<String>,
}

impl HashRing {
    pub fn new(members: &[String], replica_points: u32) -> Self {
        let mut points = Vec::with_capacity(members.len() * replica_points as usize);
        for (mi, addr) in members.iter().enumerate() {
            for r in 0..replica_points {
                let s = format!("{addr}{r}");
                points.push((fingerprint32(s.as_bytes()), mi, r));
            }
        }
        points.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| members[a.1].cmp(&members[b.1]))
                .then_with(|| a.2.cmp(&b.2))
        });
        HashRing {
            points,
            members: members.to_vec(),
        }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    fn start_index(&self, key: &str) -> usize {
        let h = fingerprint32(key.as_bytes());
        let i = self.points.partition_point(|p| p.0 < h);
        if i == self.points.len() { 0 } else { i }
    }

    /// Index (into `members`) of the owner of `key`.
    pub fn lookup(&self, key: &str) -> Option<usize> {
        if self.points.is_empty() {
            return None;
        }
        Some(self.points[self.start_index(key)].1)
    }

    /// Up to `n` distinct owners in ring order starting at `key`.
    pub fn lookup_n(&self, key: &str, n: usize) -> Vec<usize> {
        let mut out = Vec::with_capacity(n);
        if self.points.is_empty() {
            return out;
        }
        let start = self.start_index(key);
        for k in 0..self.points.len() {
            let m = self.points[(start + k) % self.points.len()].1;
            if !out.contains(&m) {
                out.push(m);
                if out.len() >= n {
                    break;
                }
            }
        }
        out
    }
}

/// Deterministic pseudo pod addresses (`10.a.b.c:port`) for a service's replicas, mimicking EKS
/// VPC CNI pod IPs. Real addresses can be supplied in the scenario instead to reproduce a live
/// cluster's exact shard placement.
pub fn synthetic_addresses(service: &str, replicas: usize, port: u16, seed: u64) -> Vec<String> {
    let mut rng = crate::sim::rng::Rng::new(seed ^ u64::from(fingerprint32(service.as_bytes())));
    let mut out = Vec::with_capacity(replicas);
    while out.len() < replicas {
        let a = 16 + rng.below(16); // 10.16.0.0/12 style private range
        let b = rng.below(256);
        let c = 1 + rng.below(254);
        let addr = format!("10.{a}.{b}.{c}:{port}");
        if !out.contains(&addr) {
            out.push(addr);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_stable_and_balanced_enough() {
        let members = synthetic_addresses("history", 6, 6934, 1);
        let ring = HashRing::new(&members, 100);
        let mut counts = vec![0usize; members.len()];
        for shard in 1..=4096 {
            counts[ring.lookup(&shard.to_string()).unwrap()] += 1;
        }
        let mean = 4096.0 / 6.0;
        for c in &counts {
            assert!(
                (*c as f64) > mean * 0.6 && (*c as f64) < mean * 1.4,
                "{counts:?}"
            );
        }
        // lookup_n returns distinct members, first equals lookup
        let n = ring.lookup_n("42", 3);
        assert_eq!(n.len(), 3);
        assert_eq!(n[0], ring.lookup("42").unwrap());
    }

    #[test]
    fn adding_a_member_moves_roughly_one_share() {
        let m6 = synthetic_addresses("history", 7, 6934, 3);
        let ring6 = HashRing::new(&m6[..6], 100);
        let ring7 = HashRing::new(&m6, 100);
        let moved = (1..=4096)
            .filter(|s| {
                let k = s.to_string();
                m6[ring6.lookup(&k).unwrap()] != m6[ring7.lookup(&k).unwrap()]
            })
            .count();
        let frac = moved as f64 / 4096.0;
        assert!(frac > 0.08 && frac < 0.25, "{frac}");
    }
}
