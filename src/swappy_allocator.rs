// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! # An [`Allocator`] implementation that has a memory budget and can be
//! swapped out.

use std::alloc::{AllocError, Allocator, Global, Layout};

use memmap2::MmapMut;
use slabmalloc::{
    AllocablePage, AllocationError, Allocator, LargeObjectPage, Pager, ZoneAllocator,
};

use crate::util::hash_u64;

pub struct SwappyAllocator<A = Global>
where
    A: Allocator,
{
    budget: usize,

    mem_usage: AtomicUsize,
    mem_allocator: A,
    swap_allocator: ZoneAllocator,

    page_allocator: FileBackedPageAllocator,
}

impl SwappyAllocator {
    pub fn new(budget: usize) -> SwappyAllocator {
        Self::new_over(Global)
    }

    pub fn new_over(budget: usize, alloc: A) -> SwappyAllocator<A> {
        SwappyAllocator {
            budget,
            mem_usage: AtomicUsize::new(0),
            mem_allocator: alloc,
            swap_allocator: ZoneAllocator::new(),
            page_allocator: FileBackedPageAllocator::new(),
        }
    }
}

unsafe impl Allocator for SwappyAllocator {
    #[inline]
    fn allocate(&self, mut layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() < 1024 {
            self.mem_usage.fetch_add(layout.size(), Ordering::Relaxed);
            return self.mem_allocator.allocate(layout);
        }
        layout = Layout::from_size_align(layout.size() + layout.align(), layout.align());
        let mut ptr;
        if self.mem_usage.load(Ordering::Relaxed) + layout.size() <= self.budget {
            self.mem_usage.fetch_add(layout.size(), Ordering::Relaxed);
            ptr = self.mem_allocator.allocate(layout)?;
            ptr.write_u8(0);
        } else if layout.size() <= MAX_SIZE {
            ptr = match self.swap_allocator.allocate(layout) {
                Err(AllocationError::OutOfMemory) => {
                    unsafe {
                        self.swap_allocator
                            .refill(layout, self.page_allocator.allocate_object_page()?)
                            .map_err(AllocError)?;
                    }
                    self.swap_allocator.allocate(layout).map_err(AllocError)?
                }
                r => r.map_err(AllocError)?,
            };
            ptr.write_u8(1);
        } else {
            ptr = self.page_allocator.allocate(layout)?;
            ptr.write_u8(2);
        };
        ptr.add(layout.align());
        ptr
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<[u8]>, layout: Layout) {
        if layout.size() < 1024 {
            self.mem_usage.fetch_sub(layout.size(), Ordering::Relaxed);
            self.mem_allocator.deallocate(ptr, layout);
        } else {
            let ptr = ptr.sub(layout.align());
            let meta = ptr.read_u8();
            match meta {
                0 => {
                    self.mem_usage.fetch_sub(layout.size(), Ordering::Relaxed);
                    self.mem_allocator.deallocate(ptr, layout);
                }
                1 => {
                    self.swap_allocator.deallocate(ptr, layout);
                }
                2 => {
                    FileBackedPageAllocator::deallocate(ptr.as_u64());
                }
            }
        }
    }
}

struct FileBackedPageAllocator {
    path: Arc<PathBuf>,
    id: AtomicU32,
}

impl FileBackedPageAllocator {
    pub fn allocate(&self, layout: Layout) -> FileBackedPage {}

    pub fn allocate_object_page(&self) -> FileBackedObjectPage {}

    pub fn deallocate(path: &Path, addr: u64) {
        let f = path.join(hash_u64(addr).to_string());
        let _ = std::fs::remove_file(&f);
    }
}

/// A block of memory that is backed by a file.
struct FileBackedPage {
    path: Arc<PathBuf>,
    f: File,
    mmap: MmapMut,
}

impl Drop for FileBackedPage {
    fn drop(&mut self) {
        self.f.close();
        FileBackedPageAllocator::deallocate(&self.path, self.mmap.as_ptr() as u64);
    }
}

/// A properly sized [`LargeObjectPage`] that holds a [`LargeObjectPage`] and
/// acts like one.
struct FileBackedObjectPage(FileBackedPage);

impl FileBackedObjectPage {
    fn as_object_page(&self) -> &LargeObjectPage {
        std::mem::transmute(self.0.as_ptr())
    }

    fn mut_object_page(&mut self) -> &mut LargeObjectPage {
        std::mem::transmute(self.0.as_ptr())
    }
}

impl AllocablePage for FileBackedObjectPage {
    fn bitfield(&self) -> &[AtomicU64; 8] {
        self.as_object_page().bitfield()
    }

    fn bitfield_mut(&mut self) -> &mut [AtomicU64; 8] {
        self.mut_object_page().bitfield_mut()
    }

    fn prev(&mut self) -> &mut Rawlink<Self>
    where
        Self: Sized,
    {
        self.mut_object_page().prev()
    }

    fn next(&mut self) -> &mut Rawlink<Self>
    where
        Self: Sized,
    {
        self.mut_object_page().next()
    }
}
