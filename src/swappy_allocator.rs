// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! # An [`Allocator`] implementation that has a memory budget and can be
//! swapped out.

use std::alloc::{AllocError, Allocator, Global, Layout};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use memmap2::MmapMut;

pub struct SwappyAllocator<A = Global>
where
    A: Allocator,
{
    budget: usize,
    path: PathBuf,

    mem_usage: AtomicUsize,
    mem_allocator: A,

    need_check_page: AtomicBool,
    page_seq: AtomicU32,
    pages: Mutex<Vec<Page>>,
}

impl<A: Allocator> SwappyAllocator<A> {
    pub fn new_over(path: &Path, budget: usize, alloc: A) -> SwappyAllocator<A> {
        SwappyAllocator {
            budget,
            path: path.into(),
            mem_usage: AtomicUsize::new(0),
            mem_allocator: alloc,
            need_check_page: AtomicBool::new(false),
            page_seq: AtomicU32::new(0),
            pages: Mutex::new(Vec::new()),
        }
    }
}

impl SwappyAllocator<Global> {
    pub fn new(path: &Path, budget: usize) -> SwappyAllocator<Global> {
        Self::new_over(path, budget, Global)
    }
}

unsafe impl Allocator for SwappyAllocator {
    #[inline]
    fn allocate(&self, mut layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mem_usage = self.mem_usage.fetch_add(layout.size(), Ordering::Relaxed);
        if mem_usage >= self.budget {
            self.mem_usage.fetch_sub(layout.size(), Ordering::Relaxed);
            let mut pages = self.pages.lock().unwrap();
            if pages.is_empty() {
                self.need_check_page.store(true, Ordering::Release);
                pages.push(Page::new(
                    &self.path,
                    self.page_seq.fetch_add(1, Ordering::Relaxed),
                    1,
                ));
            }
            match pages.last_mut().unwrap().allocate(layout) {
                None => {
                    pages.push(Page::new(
                        &self.path,
                        self.page_seq.fetch_add(1, Ordering::Relaxed),
                        1,
                    ));
                    pages.last_mut().unwrap().allocate(layout).ok_or(AllocError)
                }
                Some(r) => Ok(r),
            }
        } else {
            self.mem_allocator.allocate(layout)
        }
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if self.need_check_page.load(Ordering::Acquire) {
            let mut pages = self.pages.lock().unwrap();
            for i in 0..pages.len() {
                if pages[i].contains(ptr) {
                    if pages[i].deallocate(ptr) {
                        pages[i].release(&self.path);
                        pages.remove(i);
                    }
                    return;
                }
            }
        }
        self.mem_allocator.deallocate(ptr, layout)
    }
}

struct Page {
    seq: u32,
    f: File,
    mmap: MmapMut,
    used: usize,
    ref_counter: usize,
}

impl Page {
    fn new(root: &Path, seq: u32, size: usize) -> Page {
        let path = root.join(format!("{}.swap", seq));
        let f = File::create(&path).unwrap();
        f.set_len(size as u64);
        let mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        Self {
            seq,
            f,
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
    fn deallocate(&mut self, ptr: NonNull<u8>) -> bool {
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
