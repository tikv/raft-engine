// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, ptr::NonNull, mem};

use bytes::Bytes;

use crate::internals::FileBlockHandle;

struct CacheEntry {
    key: FileBlockHandle,
    data: Bytes,
}

struct Node {
    prev: NonNull<Node>,
    next: NonNull<Node>,
    entry: CacheEntry,
    #[cfg(test)]
    _leak_check: self::tests::LeakCheck,
}

pub struct LruCache {
    cache: HashMap<FileBlockHandle, NonNull<Node>>,

    cap: usize,
    free: usize,
    head: *mut Node,
    tail: *mut Node,

    #[cfg(test)]
    leak_check: self::tests::LeakCheck,
}

#[inline]
fn need_size(payload: &[u8]) -> usize {
    mem::size_of::<Node>() + payload.len()
}

#[inline]
unsafe fn promote(mut node: NonNull<Node>, head: &mut *mut Node, tail: &mut *mut Node) {
    if node.as_ptr() == *tail {
        return;
    }
    let mut prev = unsafe { node.as_ref().prev };
    let mut next = unsafe { node.as_ref().next };
    if node.as_ptr() == *head {
        *head = next.as_ptr();
        next.as_mut().prev = NonNull::dangling();
    } else {
        prev.as_mut().next = next;
        next.as_mut().prev = prev;
    }
    (**tail).next = node;
    node.as_mut().prev = NonNull::new_unchecked(*tail);
    node.as_mut().next = NonNull::dangling();
    *tail = node.as_ptr();
}

impl LruCache {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cache: HashMap::default(),
            cap,
            free: cap,
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
            #[cfg(test)]
            leak_check: self::tests::LeakCheck::default(),
        }
    }

    #[inline]
    pub fn insert(&mut self, key: FileBlockHandle, data: Bytes) -> Option<Bytes> {
        match self.cache.get_mut(&key) {
            None => (),
            Some(node) => {
                unsafe {
                    // Technically they should be exact the same. Using the new version
                    // to avoid potential bugs.
                    assert_eq!(data.len(), node.as_ref().entry.data.len());
                    node.as_mut().entry.data = data.clone();
                    promote(*node, &mut self.head, &mut self.tail);
                    return Some(data);
                }
            },
        }
        let need_size = need_size(&data);
        if need_size > self.cap {
            return Some(data);
        }
        while self.free < need_size && self.remove_head() {}

        let node = Box::new(Node {
            prev: NonNull::dangling(),
            next: NonNull::dangling(),
            entry: CacheEntry { key, data },
            #[cfg(test)]
            _leak_check: self.leak_check.clone(),
        });

        let node = Box::into_raw(node);

        if self.head.is_null() {
            self.head = node;
            self.tail = node;
        } else {
            unsafe {
                (*self.tail).next = NonNull::new_unchecked(node);
                (*node).prev = NonNull::new_unchecked(self.tail);
                self.tail = node;
            }
        }
        self.free -= need_size;
        self.cache.insert(key, unsafe { NonNull::new_unchecked(node) });
        None
    }

    #[inline]
    pub fn get(&mut self, key: &FileBlockHandle) -> Option<Bytes> {
        let node = self.cache.get(key)?;
        unsafe {
            promote(*node, &mut self.head, &mut self.tail);
            Some(node.as_ref().entry.data.clone())
        }
    }

    #[inline]
    fn remove_head(&mut self) -> bool {
        if self.head.is_null() {
            return false;
        }
        let mut head = unsafe { Box::from_raw(self.head) };
        if self.head != self.tail {
            self.head = head.next.as_ptr();
            head.prev = NonNull::dangling();
        } else {
            self.head = std::ptr::null_mut();
            self.tail = std::ptr::null_mut();
        }
        self.free += need_size(&head.entry.data);
        self.cache.remove(&head.entry.key);
        true
    }

    #[inline]
    pub fn resize(&mut self, new_cap: usize) {
        while (self.cap - self.free) > new_cap {
            self.remove_head();
        }
        self.free = new_cap - (self.cap - self.free);
        self.cap = new_cap;
    }
}

impl Drop for LruCache {
    fn drop(&mut self) {
        for (_, node) in self.cache.drain() {
            unsafe {
                drop(Box::from_raw(node.as_ptr()));
            }
        }
    }
}

unsafe impl Sync for LruCache {}
unsafe impl Send for LruCache {}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::{AtomicIsize, Ordering}, Arc};

    use crate::internals::LogQueue;

    use super::*;

    pub struct LeakCheck {
        cnt: Arc<AtomicIsize>,
    }

    impl LeakCheck {
        pub fn clone(&self) -> Self {
            self.cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Self {
                cnt: self.cnt.clone(),
            }
        }
    }

    impl Default for LeakCheck {
        fn default() -> Self {
            Self {
                cnt: Arc::new(AtomicIsize::new(1)),
            }
        }
    }

    impl Drop for LeakCheck {
        fn drop(&mut self) {
            self.cnt.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn test_basic_lru() {
        let mut cache = LruCache::with_capacity(1024);
        let lc = cache.leak_check.clone();
        let mut key = FileBlockHandle::dummy(LogQueue::Append);
        for offset in 0..100 {
            key.offset = offset;
            cache.insert(key, vec![offset as u8; 10].into());
        }
        for offset in 0..100 {
            key.offset = offset;
            cache.insert(key, vec![offset as u8; 10].into());
        }
        let entry_len = need_size(&[0; 10]);
        let entries_fit = (1024 / entry_len) as u64;
        assert_eq!(cache.cap, 1024);
        assert_eq!(cache.free, 1024 % entry_len);
        for offset in 0..(100 - entries_fit) {
            key.offset = offset;
            assert_eq!(cache.get(&key).as_deref(), None, "{offset}");
        }
        for offset in (100 - entries_fit)..100 {
            key.offset = offset;
            assert_eq!(cache.get(&key).as_deref(), Some(&[offset as u8; 10] as &[u8]), "{offset}");
        }

        let offset = 100 - entries_fit;
        key.offset = offset;
        // Get will promote the entry and it will be the last to be removed.
        assert_eq!(cache.get(&key).as_deref(), Some(&[offset as u8; 10] as &[u8]), "{offset}");
        for i in 1..entries_fit {
            key.offset = 200 + i;
            cache.insert(key, vec![key.offset as u8; 10].into());
            key.offset = offset + i;
            assert_eq!(cache.get(&key).as_deref(), None, "{i}");
        }
        key.offset = offset;
        assert_eq!(cache.get(&key).as_deref(), Some(&[offset as u8; 10] as &[u8]), "{offset}");

        cache.resize(2048);
        assert_eq!(cache.cap, 2048);
        assert_eq!(cache.free, 2048 - (entries_fit as usize * entry_len));
        key.offset = 201;
        assert_eq!(cache.get(&key).as_deref(), Some(&[key.offset as u8; 10] as &[u8]));
        key.offset = offset;
        assert_eq!(cache.get(&key).as_deref(), Some(&[offset as u8; 10] as &[u8]));

        cache.resize(entry_len);
        assert_eq!(cache.cap, entry_len);
        assert_eq!(cache.free, 0);
        key.offset = 201;
        assert_eq!(cache.get(&key).as_deref(), None);
        key.offset = offset;
        assert_eq!(cache.get(&key).as_deref(), Some(&[offset as u8; 10] as &[u8]));

        cache.resize(entry_len - 1);
        assert_eq!(cache.cap, entry_len - 1);
        assert_eq!(cache.free, entry_len - 1);
        key.offset = offset;
        assert_eq!(cache.get(&key).as_deref(), None);

        drop(cache);
        // If there is leak or double free, the count will unlikely to be 1.
        assert_eq!(lc.cnt.load(Ordering::Relaxed), 1);
    }
}
