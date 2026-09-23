//! Fixed-capacity LRU set keyed by `u64` (intrusive doubly linked list over a slab).
//!
//! Used for the history host-level mutable state cache (`history.hostLevelCacheMaxSize`) and
//! the SDK sticky workflow cache.

use std::collections::HashMap;

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Node {
    key: u64,
    prev: u32,
    next: u32,
}

pub struct Lru {
    cap: usize,
    map: HashMap<u64, u32>,
    nodes: Vec<Node>,
    free: Vec<u32>,
    head: u32, // most recent
    tail: u32, // least recent
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl Lru {
    pub fn new(cap: usize) -> Self {
        Lru {
            cap: cap.max(1),
            map: HashMap::with_capacity(cap.min(1 << 20)),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.cap
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
        if let Some(&i) = self.map.get(&key) {
            self.hits += 1;
            if self.head != i {
                self.unlink(i);
                self.push_front(i);
            }
            return (true, None);
        }
        self.misses += 1;
        let evicted = self.insert_new(key);
        (false, evicted)
    }

    /// Insert without counting a hit/miss (pre-warming).
    pub fn warm(&mut self, key: u64) {
        if !self.map.contains_key(&key) {
            self.insert_new(key);
        }
    }

    fn insert_new(&mut self, key: u64) -> Option<u64> {
        let mut evicted = None;
        if self.map.len() >= self.cap && self.tail != NIL {
            let t = self.tail;
            let old = self.nodes[t as usize].key;
            self.unlink(t);
            self.map.remove(&old);
            self.free.push(t);
            self.evictions += 1;
            evicted = Some(old);
        }
        let i = if let Some(i) = self.free.pop() {
            self.nodes[i as usize] = Node {
                key,
                prev: NIL,
                next: NIL,
            };
            i
        } else {
            self.nodes.push(Node {
                key,
                prev: NIL,
                next: NIL,
            });
            (self.nodes.len() - 1) as u32
        };
        self.push_front(i);
        self.map.insert(key, i);
        evicted
    }

    pub fn remove(&mut self, key: u64) -> bool {
        if let Some(i) = self.map.remove(&key) {
            self.unlink(i);
            self.free.push(i);
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
}
