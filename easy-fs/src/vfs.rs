use super::{
    block_cache_sync_all, get_block_cache, BlockDevice, DirEntry, DiskInode, DiskInodeType,
    EasyFileSystem, DIRENT_SZ,
};
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use spin::{Mutex, MutexGuard};

/// Virtual filesystem layer over easy-fs
pub struct Inode {
    inode_id: u32,
    block_id: usize,
    block_offset: usize,
    fs: Arc<Mutex<EasyFileSystem>>,
    block_device: Arc<dyn BlockDevice>,
}

impl Inode {
    /// Create a vfs inode
    pub fn new(
        inode_id: u32,
        block_id: u32,
        block_offset: usize,
        fs: Arc<Mutex<EasyFileSystem>>,
        block_device: Arc<dyn BlockDevice>,
    ) -> Self {
        Self {
            inode_id,
            block_id: block_id as usize,
            block_offset,
            fs,
            block_device,
        }
    }
    /// Call a function over a disk inode to read it
    fn read_disk_inode<V>(&self, f: impl FnOnce(&DiskInode) -> V) -> V {
        get_block_cache(self.block_id, Arc::clone(&self.block_device))
            .lock()
            .read(self.block_offset, f)
    }
    /// Call a function over a disk inode to modify it
    fn modify_disk_inode<V>(&self, f: impl FnOnce(&mut DiskInode) -> V) -> V {
        get_block_cache(self.block_id, Arc::clone(&self.block_device))
            .lock()
            .modify(self.block_offset, f)
    }
    /// Find inode under a disk inode by name
    fn find_inode_id(&self, name: &str, disk_inode: &DiskInode) -> Option<u32> {
        // assert it is a directory
        assert!(disk_inode.is_dir());
        let file_count = (disk_inode.size as usize) / DIRENT_SZ;
        let mut dirent = DirEntry::empty();
        for i in 0..file_count {
            assert_eq!(
                disk_inode.read_at(DIRENT_SZ * i, dirent.as_bytes_mut(), &self.block_device,),
                DIRENT_SZ,
            );
            if dirent.name() == name {
                return Some(dirent.inode_number() as u32);
            }
        }
        None
    }
    /// Find inode under current inode by name
    pub fn find(&self, name: &str) -> Option<Arc<Inode>> {
        let fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            self.find_inode_id(name, disk_inode).map(|inode_id| {
                let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);
                Arc::new(Self::new(
                    inode_id,
                    block_id,
                    block_offset,
                    self.fs.clone(),
                    self.block_device.clone(),
                ))
            })
        })
    }
    /// Increase the size of a disk inode
    fn increase_size(
        &self,
        new_size: u32,
        disk_inode: &mut DiskInode,
        fs: &mut MutexGuard<EasyFileSystem>,
    ) {
        if new_size <= disk_inode.size {
            return;
        }
        let blocks_needed = disk_inode.blocks_num_needed(new_size);
        let mut v: Vec<u32> = Vec::new();
        for _ in 0..blocks_needed {
            v.push(fs.alloc_data());
        }
        disk_inode.increase_size(new_size, v, &self.block_device);
    }
    /// Create inode under current inode by name
    pub fn create(&self, name: &str) -> Option<Arc<Inode>> {
        let mut fs = self.fs.lock();
        if self
            .modify_disk_inode(|root_inode| {
                // assert it is a directory
                assert!(root_inode.is_dir());
                // has the file been created?
                self.find_inode_id(name, root_inode)
            })
            .is_some()
        {
            return None;
        }
        // create a new file
        // alloc a inode with an indirect block
        let new_inode_id = fs.alloc_inode();
        // initialize inode
        let (new_inode_block_id, new_inode_block_offset) = fs.get_disk_inode_pos(new_inode_id);
        get_block_cache(new_inode_block_id as usize, Arc::clone(&self.block_device))
            .lock()
            .modify(new_inode_block_offset, |new_inode: &mut DiskInode| {
                new_inode.initialize(DiskInodeType::File);
            });
        self.modify_disk_inode(|root_inode| {
            // append file in the dirent
            let mut file_count = (root_inode.size as usize) / DIRENT_SZ;
            let mut last_dirent = DirEntry::empty();
            if file_count > 0 {
                root_inode.read_at(
                    (file_count - 1) * DIRENT_SZ,
                    last_dirent.as_bytes_mut(),
                    &self.block_device,
                );
                if !last_dirent.is_empty() {
                    file_count += 1;
                }
            } else {
                file_count += 1;
            }

            // increase size
            let new_size = file_count * DIRENT_SZ;
            self.increase_size(new_size as u32, root_inode, &mut fs);
            // write dirent
            let dirent = DirEntry::new(name, new_inode_id);
            root_inode.write_at(
                (file_count - 1) * DIRENT_SZ,
                dirent.as_bytes(),
                &self.block_device,
            );
        });

        let (block_id, block_offset) = fs.get_disk_inode_pos(new_inode_id);
        block_cache_sync_all();
        // return inode
        Some(Arc::new(Self::new(
            new_inode_id,
            block_id,
            block_offset,
            self.fs.clone(),
            self.block_device.clone(),
        )))
        // release efs lock automatically by compiler
    }
    /// Get Stat
    pub fn get_stat(&self) -> (u32, bool, bool, u32) {
        let ino = self.inode_id;
        let (is_dir, is_file, nlink) = self.read_disk_inode(|d| (d.is_dir(), d.is_file(), d.nlink));
        (ino, is_dir, is_file, nlink)
    }
    /// Link a file to the given inode
    pub fn link(&self, name: &str, inode: Arc<Inode>) {
        let mut fs = self.fs.lock();

        // update nlink
        inode.modify_disk_inode(|disk_inode: &mut DiskInode| {
            disk_inode.nlink += 1;
        });

        // update root inode
        self.modify_disk_inode(|root_inode| {
            let mut file_count = (root_inode.size as usize) / DIRENT_SZ;
            let mut last_dirent = DirEntry::empty();
            if file_count > 0 {
                // read the last dir entry
                root_inode.read_at(
                    (file_count - 1) * DIRENT_SZ,
                    last_dirent.as_bytes_mut(),
                    &self.block_device,
                );
                if !last_dirent.is_empty() {
                    file_count += 1;
                }
            } else {
                file_count += 1;
            }
            // increase size
            let new_size = file_count * DIRENT_SZ;
            self.increase_size(new_size as u32, root_inode, &mut fs);
            // write dirent
            let dirent = DirEntry::new(name, inode.inode_id);
            root_inode.write_at(
                (file_count - 1) * DIRENT_SZ,
                dirent.as_bytes(),
                &self.block_device,
            );
        });

        // let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);

        block_cache_sync_all();
    }

    /// unlink a file
    pub fn unlink(&self, name: &str) -> isize {
        let mut fs = self.fs.lock();
        // find the inode
        if let Some(inode) = self.read_disk_inode(|disk_inode| {
            self.find_inode_id(name, disk_inode).map(|inode_id| {
                let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);
                Arc::new(Self::new(
                    inode_id,
                    block_id,
                    block_offset,
                    self.fs.clone(),
                    self.block_device.clone(),
                ))
            })
        }) {
            let nlink = inode.modify_disk_inode(|disk_inode: &mut DiskInode| {
                assert_ne!(disk_inode.nlink, 0);
                disk_inode.nlink -= 1;
                disk_inode.nlink
            });
            debug!("nlink = {}", nlink);
            if nlink == 0 {
                // dealloc this inode
                fs.dealloc_inode(inode.inode_id);
            }
        } else {
            return -1;
        }

        debug!("update root inode");
        // update root inode
        self.modify_disk_inode(|root_inode| {
            let file_count = (root_inode.size as usize) / DIRENT_SZ;

            // remove the dir entry
            let mut dirent = DirEntry::empty();
            for i in 0..file_count {
                assert_eq!(
                    root_inode.read_at(DIRENT_SZ * i, dirent.as_bytes_mut(), &self.block_device,),
                    DIRENT_SZ,
                );
                if dirent.name() == name {
                    // invalidate this file
                    let empty = DirEntry::empty();
                    let mut buf = [0u8; DIRENT_SZ];
                    assert_eq!(
                        root_inode.read_at(
                            DIRENT_SZ * (file_count - 1),
                            &mut buf,
                            &self.block_device
                        ),
                        DIRENT_SZ
                    );
                    // overwrite this dir entry with the last dir entry
                    assert_eq!(
                        root_inode.write_at(DIRENT_SZ * i, &buf, &self.block_device),
                        DIRENT_SZ,
                    );
                    // empty the last dir entry
                    assert_eq!(
                        root_inode.write_at(
                            DIRENT_SZ * (file_count - 1),
                            empty.as_bytes(),
                            &self.block_device
                        ),
                        DIRENT_SZ,
                    );
                    break;
                }
            }
        });

        // block_cache_sync_all();
        0
    }

    /// List inodes under current inode
    pub fn ls(&self) -> Vec<String> {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            let file_count = (disk_inode.size as usize) / DIRENT_SZ;
            let mut v: Vec<String> = Vec::new();
            for i in 0..file_count {
                let mut dirent = DirEntry::empty();
                assert_eq!(
                    disk_inode.read_at(i * DIRENT_SZ, dirent.as_bytes_mut(), &self.block_device,),
                    DIRENT_SZ,
                );
                v.push(String::from(dirent.name()));
            }
            v
        })
    }
    /// Read data from current inode
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> usize {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| disk_inode.read_at(offset, buf, &self.block_device))
    }
    /// Write data to current inode
    pub fn write_at(&self, offset: usize, buf: &[u8]) -> usize {
        let mut fs = self.fs.lock();
        let size = self.modify_disk_inode(|disk_inode| {
            self.increase_size((offset + buf.len()) as u32, disk_inode, &mut fs);
            disk_inode.write_at(offset, buf, &self.block_device)
        });
        block_cache_sync_all();
        size
    }
    /// Clear the data in current inode
    pub fn clear(&self) {
        let mut fs = self.fs.lock();
        self.modify_disk_inode(|disk_inode| {
            let size = disk_inode.size;
            let data_blocks_dealloc = disk_inode.clear_size(&self.block_device);
            assert!(data_blocks_dealloc.len() == DiskInode::total_blocks(size) as usize);
            for data_block in data_blocks_dealloc.into_iter() {
                fs.dealloc_data(data_block);
            }
        });
        block_cache_sync_all();
    }
}
