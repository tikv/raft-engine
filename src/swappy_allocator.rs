// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! # An [`Allocator`] implementation that has a memory budget and can be
//! swapped out.

use std::alloc::{AllocError, Allocator, Global, Layout};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use memmap2::MmapMut;

const DEFAULT_PAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB

pub struct SwappyAllocatorCore<A = Global>
where
    A: Allocator,
{
    budget: usize,
    path: PathBuf,

    mem_usage: AtomicUsize,
    mem_allocator: A,

    maybe_swapped: AtomicBool,
    page_seq: AtomicU32,
    pages: Mutex<Vec<Page>>,
}

#[derive(Clone)]
pub struct SwappyAllocator<A: Allocator>(Arc<SwappyAllocatorCore<A>>);

impl<A: Allocator> SwappyAllocator<A> {
    pub fn new_over(path: &Path, budget: usize, alloc: A) -> SwappyAllocator<A> {
        let core = SwappyAllocatorCore {
            budget,
            path: path.into(),
            mem_usage: AtomicUsize::new(0),
            mem_allocator: alloc,
            maybe_swapped: AtomicBool::new(false),
            page_seq: AtomicU32::new(0),
            pages: Mutex::new(Vec::new()),
        };
        SwappyAllocator(Arc::new(core))
    }

    #[inline]
    pub fn memory_usage(&self) -> usize {
        self.0.mem_usage.load(Ordering::Relaxed)
    }

    #[inline]
    fn allocate_swapped(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mut pages = self.0.pages.lock().unwrap();
        if pages.is_empty() {
            self.0.maybe_swapped.store(true, Ordering::Release);
            pages.push(Page::new(
                &self.0.path,
                self.0.page_seq.fetch_add(1, Ordering::Relaxed),
                std::cmp::max(DEFAULT_PAGE_SIZE, layout.size()),
            ));
        }
        match pages.last_mut().unwrap().allocate(layout) {
            None => {
                pages.push(Page::new(
                    &self.0.path,
                    self.0.page_seq.fetch_add(1, Ordering::Relaxed),
                    std::cmp::max(DEFAULT_PAGE_SIZE, layout.size()),
                ));
                pages.last_mut().unwrap().allocate(layout).ok_or(AllocError)
            }
            Some(r) => Ok(r),
        }
    }
}

impl SwappyAllocator<Global> {
    pub fn new(path: &Path, budget: usize) -> SwappyAllocator<Global> {
        Self::new_over(path, budget, Global)
    }
}

unsafe impl<A: Allocator> Allocator for SwappyAllocator<A> {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mem_usage = self.0.mem_usage.fetch_add(layout.size(), Ordering::Relaxed);
        if mem_usage >= self.0.budget {
            self.0.mem_usage.fetch_sub(layout.size(), Ordering::Relaxed);
            if layout.size() == 0 {
                Ok(NonNull::slice_from_raw_parts(layout.dangling(), 0))
            } else {
                self.allocate_swapped(layout)
            }
        } else {
            self.0.mem_allocator.allocate(layout)
        }
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if self.0.maybe_swapped.load(Ordering::Acquire) {
            let mut pages = self.0.pages.lock().unwrap();
            for i in 0..pages.len() {
                if pages[i].contains(ptr) {
                    if pages[i].deallocate(ptr) {
                        pages[i].release(&self.0.path);
                        pages.remove(i);
                    }
                    if pages.is_empty() {
                        self.0.maybe_swapped.store(false, Ordering::Release);
                    }
                    return;
                }
            }
        }
        self.0.mem_usage.fetch_sub(layout.size(), Ordering::Relaxed);
        self.0.mem_allocator.deallocate(ptr, layout)
    }

    #[inline]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = self.allocate(layout)?;
        unsafe { ptr.as_non_null_ptr().as_ptr().write_bytes(0, ptr.len()) }
        Ok(ptr)
    }

    #[inline]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let diff = new_layout.size() - old_layout.size();
        let mem_usage = self.0.mem_usage.fetch_add(diff, Ordering::Relaxed);
        if mem_usage >= self.0.budget || self.0.maybe_swapped.load(Ordering::Acquire) {
            self.0.mem_usage.fetch_sub(diff, Ordering::Relaxed);
            // Copied from std's blanket implementation.
            debug_assert!(
                new_layout.size() >= old_layout.size(),
                "`new_layout.size()` must be greater than or equal to `old_layout.size()`"
            );

            let new_ptr = self.allocate(new_layout)?;

            // SAFETY: because `new_layout.size()` must be greater than or equal to
            // `old_layout.size()`, both the old and new memory allocation are valid for
            // reads and writes for `old_layout.size()` bytes. Also, because the
            // old allocation wasn't yet deallocated, it cannot overlap
            // `new_ptr`. Thus, the call to `copy_nonoverlapping` is
            // safe. The safety contract for `dealloc` must be upheld by the caller.
            unsafe {
                ptr::copy_nonoverlapping(ptr.as_ptr(), new_ptr.as_mut_ptr(), old_layout.size());
                self.deallocate(ptr, old_layout);
            }

            Ok(new_ptr)
        } else {
            self.0.mem_allocator.grow(ptr, old_layout, new_layout)
        }
    }

    #[inline]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = self.grow(ptr, old_layout, new_layout)?;
        unsafe { ptr.as_non_null_ptr().as_ptr().write_bytes(0, ptr.len()) }
        Ok(ptr)
    }

    #[inline]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if self.0.maybe_swapped.load(Ordering::Acquire) {
            // Copied from std's blanket implementation.
            debug_assert!(
                new_layout.size() <= old_layout.size(),
                "`new_layout.size()` must be smaller than or equal to `old_layout.size()`"
            );

            let new_ptr = self.allocate(new_layout)?;

            // SAFETY: because `new_layout.size()` must be lower than or equal to
            // `old_layout.size()`, both the old and new memory allocation are valid for
            // reads and writes for `new_layout.size()` bytes. Also, because the
            // old allocation wasn't yet deallocated, it cannot overlap
            // `new_ptr`. Thus, the call to `copy_nonoverlapping` is
            // safe. The safety contract for `dealloc` must be upheld by the caller.
            unsafe {
                ptr::copy_nonoverlapping(ptr.as_ptr(), new_ptr.as_mut_ptr(), new_layout.size());
                self.deallocate(ptr, old_layout);
            }

            Ok(new_ptr)
        } else {
            self.0
                .mem_usage
                .fetch_sub(old_layout.size() - new_layout.size(), Ordering::Relaxed);
            self.0.mem_allocator.shrink(ptr, old_layout, new_layout)
        }
    }
}

struct Page {
    seq: u32,
    _f: File,
    mmap: MmapMut,
    used: usize,
    ref_counter: usize,
}

impl Page {
    fn new(root: &Path, seq: u32, size: usize) -> Page {
        let path = root.join(format!("{}.swap", seq));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        f.set_len(size as u64).unwrap();
        let mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        Self {
            seq,
            _f: f,
            mmap,
            used: 0,
            ref_counter: 0,
        }
    }

    fn allocate(&mut self, layout: Layout) -> Option<NonNull<[u8]>> {
        unsafe {
            let offset = self
                .mmap
                .as_ptr()
                .add(self.used)
                .align_offset(layout.align());
            if self.used + offset + layout.size() > self.mmap.len() {
                None
            } else {
                let p = self.mmap.as_ptr().add(self.used + offset);
                self.used += offset + layout.size();
                self.ref_counter += 1;
                Some(NonNull::slice_from_raw_parts(
                    NonNull::new_unchecked(p as *mut u8),
                    layout.size(),
                ))
            }
        }
    }

    // Returns whether the page can be retired.
    fn deallocate(&mut self, _ptr: NonNull<u8>) -> bool {
        self.ref_counter -= 1;
        self.ref_counter == 0
    }

    fn release(&mut self, root: &Path) {
        let path = root.join(format!("{}.swap", self.seq));
        std::fs::remove_file(&path).unwrap();
    }

    fn contains(&self, ptr: NonNull<u8>) -> bool {
        unsafe {
            let start = self.mmap.as_ptr();
            let end = (self.mmap.as_ptr()).add(self.mmap.len());
            let ptr = ptr.as_ptr() as *const u8;
            ptr >= start && ptr < end
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_count(p: &Path) -> usize {
        let mut files = 0;
        p.read_dir().unwrap().for_each(|_| files += 1);
        files
    }

    #[test]
    fn test_swappy_vec() {
        let dir = tempfile::Builder::new()
            .prefix("test_swappy_vec")
            .tempdir()
            .unwrap();

        let allocator = SwappyAllocator::new(dir.path(), 1024);
        let mut vec1: Vec<u8, _> = Vec::new_in(allocator.clone());
        assert_eq!(allocator.memory_usage(), 0);
        vec1.resize(1024, 0);
        assert_eq!(allocator.memory_usage(), 1024);
        // Small vec that uses swap.
        let mut vec2: Vec<u8, _> = Vec::new_in(allocator.clone());
        vec2.resize(16, 0);
        assert_eq!(allocator.memory_usage(), 1024);
        // Grow large vec to swap.
        vec1.resize(2048, 0);
        assert_eq!(allocator.memory_usage(), 0);
        // Shrink large vec to free up memory.
        vec1.truncate(4);
        vec1.shrink_to_fit();
        assert_eq!(allocator.memory_usage(), 4);
        // Grow small vec, should be in memory.
        vec2.resize(32, 0);
        assert_eq!(allocator.memory_usage(), 4 + 32);

        assert_eq!(file_count(dir.path()), 0);
    }

    #[test]
    fn test_page_refill() {
        let dir = tempfile::Builder::new()
            .prefix("test_page_refill")
            .tempdir()
            .unwrap();

        let allocator = SwappyAllocator::new(dir.path(), 0);

        let mut vec: Vec<u8, _> = Vec::new_in(allocator.clone());
        assert_eq!(allocator.memory_usage(), 0);
        vec.resize(DEFAULT_PAGE_SIZE, 0);
        assert_eq!(allocator.memory_usage(), 0);
        assert_eq!(file_count(dir.path()), 1);
        vec.resize(DEFAULT_PAGE_SIZE * 2, 0);
        assert_eq!(allocator.memory_usage(), 0);
        assert_eq!(file_count(dir.path()), 1);
        vec.resize(DEFAULT_PAGE_SIZE * 3, 0);
        assert_eq!(allocator.memory_usage(), 0);
        assert_eq!(file_count(dir.path()), 1);
        vec.clear();
        vec.shrink_to_fit();
        assert_eq!(allocator.memory_usage(), 0);
        assert_eq!(file_count(dir.path()), 0);
    }

    #[test]
    fn test_empty_pointer() {
        let dir = tempfile::Builder::new()
            .prefix("test_empty_pointer")
            .tempdir()
            .unwrap();
        let allocator = SwappyAllocator::new(dir.path(), 0);
        let _ = allocator
            .allocate(Layout::from_size_align(0, 1).unwrap())
            .unwrap();
        assert_eq!(allocator.memory_usage(), 0);
        assert_eq!(file_count(dir.path()), 0);
    }

    fn bench_allocator<A: Allocator>(a: &A) {
        unsafe {
            let ptr = a
                .allocate_zeroed(Layout::from_size_align(8, 8).unwrap())
                .unwrap();
            let ptr = a
                .grow_zeroed(
                    NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                    Layout::from_size_align(8, 8).unwrap(),
                    Layout::from_size_align(16, 8).unwrap(),
                )
                .unwrap();
            let ptr = a
                .grow_zeroed(
                    NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                    Layout::from_size_align(16, 8).unwrap(),
                    Layout::from_size_align(32, 8).unwrap(),
                )
                .unwrap();
            let ptr = a
                .grow_zeroed(
                    NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                    Layout::from_size_align(32, 8).unwrap(),
                    Layout::from_size_align(64, 8).unwrap(),
                )
                .unwrap();
            let ptr = a
                .grow_zeroed(
                    NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                    Layout::from_size_align(64, 8).unwrap(),
                    Layout::from_size_align(128, 8).unwrap(),
                )
                .unwrap();
            let ptr = a
                .shrink(
                    NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                    Layout::from_size_align(128, 8).unwrap(),
                    Layout::from_size_align(8, 8).unwrap(),
                )
                .unwrap();
            a.deallocate(
                NonNull::new_unchecked(ptr.as_ptr().as_mut_ptr()),
                Layout::from_size_align(8, 8).unwrap(),
            );
        }
    }

    #[bench]
    fn bench_allocator_std_global(b: &mut test::Bencher) {
        b.iter(move || {
            bench_allocator(&std::alloc::Global);
        });
    }

    #[bench]
    fn bench_allocator_fast_path(b: &mut test::Bencher) {
        let dir = tempfile::Builder::new()
            .prefix("bench_fast_path")
            .tempdir()
            .unwrap();
        let allocator = SwappyAllocator::new(dir.path(), usize::MAX);
        b.iter(move || {
            bench_allocator(&allocator);
        });
    }

    #[bench]
    fn bench_allocator_slow_path(b: &mut test::Bencher) {
        let dir = tempfile::Builder::new()
            .prefix("bench_fast_path")
            .tempdir()
            .unwrap();
        let allocator = SwappyAllocator::new(dir.path(), 0);
        b.iter(move || {
            bench_allocator(&allocator);
        });
    }
}
