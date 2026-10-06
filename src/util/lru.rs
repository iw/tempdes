//! Fixed-capacity LRU set keyed by `u64` (intrusive doubly linked list over a slab).
//!
//! Used for the history host-level mutable state cache (`history.hostLevelCacheMaxSize`, with
//! `history.cacheTTL`) and the SDK sticky workflow cache, whose entries count one each, and for
//! the shard events cache, whose entries weigh their size in bytes
//! (`history.eventsCacheMaxSizeBytes`).

use std::collections::HashMap;

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Node {
    key: u64,
    size: u64,
    /// when the entry was inserted, for the TTL
    at: u64,
    prev: u32,
    next: u32,
}

pub struct Lru {
    /// capacity in entries, or in bytes where entries have sizes
    cap: u64,
    used: u64,
    map: HashMap<u64, u32>,
    nodes: Vec<Node>,
    free: Vec<u32>,
    head: u32, // most recent
    tail: u32, // least recent
    /// entries expire this long after they were inserted (0: never)
    ttl: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl Lru {
    /// A cache of `cap` entries.
    pub fn new(cap: usize) -> Self {
        Lru {
            map: HashMap::with_capacity(cap.min(1 << 20)),
            ..Lru::sized(cap as u64)
        }
    }

    /// A cache of `cap` bytes, for entries `put` with their sizes; it grows as they arrive
    /// rather than reserving room up front.
    pub fn sized(cap: u64) -> Self {
        Lru {
            cap: cap.max(1),
            used: 0,
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            ttl: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    /// Expire entries `ttl` after they were inserted, however often they are read since
    /// (`common/cache/lru.go`: only a put that replaces an entry's value renews it).
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    fn unlink(&mut self, i: u32) {
        let n = self.nodes[i as usize];
        if n.prev != NIL {
            self.nodes[n.prev as usize].next = n.next;
        } else {
            self.head = n.next;
        }
        if n.next != NIL {
            self.nodes[n.next as usize].prev = n.prev;
        } else {
            self.tail = n.prev;
        }
    }

    fn push_front(&mut self, i: u32) {
        self.nodes[i as usize].prev = NIL;
        self.nodes[i as usize].next = self.head;
        if self.head != NIL {
            self.nodes[self.head as usize].prev = i;
        }
        self.head = i;
        if self.tail == NIL {
            self.tail = i;
        }
    }

    pub fn contains(&self, key: u64) -> bool {
        self.map.contains_key(&key)
    }

    /// Access `key`: returns true on hit. On miss the key is inserted (evicting the LRU entry
    /// when full) and the evicted key, if any, is returned via `evicted`.
    pub fn access(&mut self, key: u64) -> (bool, Option<u64>) {
        self.access_at(key, 0)
    }

    /// `access` at time `now`. An entry older than the TTL is a miss and starts over, as the
    /// lookup that finds it expired deletes it and the caller puts a new one.
    pub fn access_at(&mut self, key: u64, now: u64) -> (bool, Option<u64>) {
        if let Some(&i) = self.map.get(&key) {
            let expired = self.ttl > 0 && now.saturating_sub(self.nodes[i as usize].at) > self.ttl;
            if expired {
                self.misses += 1;
                self.nodes[i as usize].at = now;
            } else {
                self.hits += 1;
            }
            if self.head != i {
                self.unlink(i);
                self.push_front(i);
            }
            return (!expired, None);
        }
        self.misses += 1;
        let evicted = self.insert_new(key, 1, now);
        (false, evicted)
    }

    /// Insert without counting a hit/miss (pre-warming).
    pub fn warm(&mut self, key: u64) {
        if !self.map.contains_key(&key) {
            self.insert_new(key, 1, 0);
        }
    }

    /// Look `key` up, counting a hit or a miss; a hit makes it the most recent.
    pub fn get(&mut self, key: u64) -> bool {
        if let Some(&i) = self.map.get(&key) {
            self.hits += 1;
            if self.head != i {
                self.unlink(i);
                self.push_front(i);
            }
            return true;
        }
        self.misses += 1;
        false
    }

    /// Insert `key` weighing `size`, evicting the least recent entries to make room. An entry
    /// larger than the whole capacity isn't kept.
    pub fn put(&mut self, key: u64, size: u64) {
        self.remove(key);
        if size <= self.cap {
            self.insert_new(key, size, 0);
        }
    }

    fn insert_new(&mut self, key: u64, size: u64, at: u64) -> Option<u64> {
        let mut evicted = None;
        while self.used + size > self.cap && self.tail != NIL {
            let t = self.tail;
            let old = self.nodes[t as usize];
            self.unlink(t);
            self.map.remove(&old.key);
            self.free.push(t);
            self.used -= old.size;
            self.evictions += 1;
            evicted = Some(old.key);
        }
        let node = Node {
            key,
            size,
            at,
            prev: NIL,
            next: NIL,
        };
        let i = if let Some(i) = self.free.pop() {
            self.nodes[i as usize] = node;
            i
        } else {
            self.nodes.push(node);
            (self.nodes.len() - 1) as u32
        };
        self.push_front(i);
        self.map.insert(key, i);
        self.used += size;
        evicted
    }

    pub fn remove(&mut self, key: u64) -> bool {
        if let Some(i) = self.map.remove(&key) {
            self.unlink(i);
            self.free.push(i);
            self.used -= self.nodes[i as usize].size;
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.nodes.clear();
        self.free.clear();
        self.head = NIL;
        self.tail = NIL;
        self.used = 0;
    }

    pub fn reset_stats(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
    }

    pub fn hit_ratio(&self) -> f64 {
        let t = self.hits + self.misses;
        if t == 0 {
            1.0
        } else {
            self.hits as f64 / t as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_expire_from_insertion_however_often_read() {
        let mut c = Lru::new(4).with_ttl(10);
        assert_eq!(c.access_at(1, 0), (false, None));
        assert_eq!(c.access_at(1, 5), (true, None));
        assert_eq!(c.access_at(1, 10), (true, None));
        // read at 5 and 10, but inserted at 0: expired at 11, and inserted afresh
        assert_eq!(c.access_at(1, 11), (false, None));
        assert_eq!(c.access_at(1, 21), (true, None));
        assert_eq!(c.access_at(1, 22), (false, None));
        assert_eq!((c.hits, c.misses, c.len()), (3, 3, 1));
        // without a TTL nothing expires
        let mut c = Lru::new(4);
        c.access_at(1, 0);
        assert_eq!(c.access_at(1, u64::MAX), (true, None));
    }

    #[test]
    fn lru_evicts_least_recent() {
        let mut c = Lru::new(2);
        assert_eq!(c.access(1), (false, None));
        assert_eq!(c.access(2), (false, None));
        assert_eq!(c.access(1), (true, None));
        assert_eq!(c.access(3), (false, Some(2)));
        assert!(c.contains(1) && c.contains(3) && !c.contains(2));
        assert!(c.remove(1));
        assert_eq!(c.access(4), (false, None));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn sized_entries_evict_until_they_fit() {
        let mut c = Lru::sized(1000);
        c.put(1, 400);
        c.put(2, 400);
        assert!(c.get(1));
        // 1 is the most recent: 2 goes to make room
        c.put(3, 500);
        assert!(c.contains(1) && c.contains(3) && !c.contains(2));
        assert!(!c.get(2));
        // an entry larger than the cache isn't kept
        c.put(4, 2000);
        assert!(!c.contains(4) && c.contains(1));
        assert_eq!((c.hits, c.misses), (1, 1));
    }
}
