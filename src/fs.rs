use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Notify};

use crate::device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;
pub const VIRTIO_FS_TAG_SIZE: usize = 36;

const FUSE_ROOT_ID: u64 = 1;
const FUSE_LOOKUP: u32 = 1;
const FUSE_FORGET: u32 = 2;
const FUSE_GETATTR: u32 = 3;
const FUSE_OPEN: u32 = 14;
const FUSE_READ: u32 = 15;
const FUSE_RELEASE: u32 = 18;
const FUSE_STATFS: u32 = 17;
const FUSE_INIT: u32 = 26;
const FUSE_OPENDIR: u32 = 27;
const FUSE_READDIR: u32 = 28;
const FUSE_RELEASEDIR: u32 = 29;
const FUSE_DESTROY: u32 = 38;
const FUSE_IN_HEADER_SIZE: usize = 40;
const FUSE_OUT_HEADER_SIZE: usize = 16;
const FUSE_INIT_OUT_SIZE: usize = 64;
const FUSE_ATTR_SIZE: usize = 88;
const FUSE_ENTRY_OUT_SIZE: usize = 128;
const FUSE_ATTR_OUT_SIZE: usize = 104;
const FUSE_OPEN_OUT_SIZE: usize = 16;
const FUSE_STATFS_OUT_SIZE: usize = 80;
const FUSE_MIN_VERSION: u32 = 7;
const FUSE_MIN_MINOR: u32 = 31;
const FUSE_MAX_READ: usize = 1024 * 1024;
const VIRTQ_DESC_F_WRITE: u16 = 2;
const MAXIMUM_INFLIGHT_PER_QUEUE: usize = 4;
const MAXIMUM_INFLIGHT_REQUESTS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsConfig {
    pub tag: [u8; VIRTIO_FS_TAG_SIZE],
    pub request_queue_count: u32,
}

#[derive(Clone, Debug)]
pub struct FsDeclaration {
    root: PathBuf,
    tag: [u8; VIRTIO_FS_TAG_SIZE],
    request_queue_count: usize,
    maximum_queue_size: u16,
}

impl FsDeclaration {
    pub fn open(
        root: impl Into<PathBuf>,
        tag: &str,
        request_queue_count: usize,
        maximum_queue_size: u16,
    ) -> Result<Self, DeviceError> {
        let root = fs::canonicalize(root.into())?;
        if !root.is_dir() {
            return Err(DeviceError::InvalidLayout(
                "virtio-fs root is not a directory",
            ));
        }
        if tag.is_empty() || tag.len() > VIRTIO_FS_TAG_SIZE || !tag.is_char_boundary(tag.len()) {
            return Err(DeviceError::InvalidLayout("invalid virtio-fs tag"));
        }
        if request_queue_count == 0 || request_queue_count > usize::from(u16::MAX - 1) {
            return Err(DeviceError::InvalidLayout(
                "invalid virtio-fs request queue count",
            ));
        }
        let mut tag_bytes = [0u8; VIRTIO_FS_TAG_SIZE];
        tag_bytes[..tag.len()].copy_from_slice(tag.as_bytes());
        Ok(Self {
            root,
            tag: tag_bytes,
            request_queue_count,
            maximum_queue_size,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> FsConfig {
        FsConfig {
            tag: self.tag,
            request_queue_count: u32::try_from(self.request_queue_count)
                .expect("validated request queue count"),
        }
    }

    pub fn config_bytes(&self) -> [u8; 40] {
        let config = self.config();
        let mut bytes = [0u8; 40];
        bytes[..VIRTIO_FS_TAG_SIZE].copy_from_slice(&config.tag);
        bytes[36..40].copy_from_slice(&config.request_queue_count.to_le_bytes());
        bytes
    }
}

#[derive(Clone)]
struct FsInodes {
    next: u64,
    by_id: HashMap<u64, PathBuf>,
    by_path: HashMap<PathBuf, u64>,
}

impl FsInodes {
    fn new(root: PathBuf) -> Self {
        let mut by_id = HashMap::new();
        let mut by_path = HashMap::new();
        by_id.insert(FUSE_ROOT_ID, root.clone());
        by_path.insert(root, FUSE_ROOT_ID);
        Self {
            next: FUSE_ROOT_ID + 1,
            by_id,
            by_path,
        }
    }

    fn id_for_path(&mut self, path: PathBuf) -> Result<u64, DeviceError> {
        if let Some(id) = self.by_path.get(&path) {
            return Ok(*id);
        }
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or(DeviceError::InvalidLayout("virtio-fs inode id overflow"))?;
        self.by_id.insert(id, path.clone());
        self.by_path.insert(path, id);
        Ok(id)
    }
}

pub struct FsDevice {
    root: PathBuf,
    resources: DeviceResources,
    queue_states: Mutex<Vec<QueueState>>,
    inodes: Arc<std::sync::Mutex<FsInodes>>,
    wake: Notify,
    down: AtomicBool,
}

struct FsWork {
    queue_index: usize,
    chain: DescriptorChain,
    request: Vec<u8>,
    reply: Vec<DmaRange>,
}

struct FsCompletion {
    work: FsWork,
    reply: Vec<u8>,
}

impl FsDevice {
    fn new(declaration: &FsDeclaration, resources: DeviceResources) -> Self {
        Self {
            root: declaration.root.clone(),
            queue_states: Mutex::new(vec![QueueState::new(); resources.queues.len()]),
            inodes: Arc::new(std::sync::Mutex::new(FsInodes::new(
                declaration.root.clone(),
            ))),
            resources,
            wake: Notify::new(),
            down: AtomicBool::new(false),
        }
    }

    fn check_live(&self) -> Result<(), DeviceError> {
        if self.down.load(Ordering::Acquire) {
            return Err(DeviceError::Down(DeviceDownReason::Revoked));
        }
        Ok(())
    }

    async fn take_work(&self, queue_index: usize) -> Result<Option<FsWork>, DeviceError> {
        let queue_layout = self.resources.queues[queue_index];
        let queue = VirtQueue::new(queue_layout, &self.resources.dma)?;
        let chain = {
            let mut states = self.queue_states.lock().await;
            queue.pop(&self.resources.dma, &mut states[queue_index])?
        };
        let Some(chain) = chain else {
            return Ok(None);
        };
        let (request, reply) = parse_chain(&self.resources.dma, &chain)?;
        Ok(Some(FsWork {
            queue_index,
            chain,
            request,
            reply,
        }))
    }

    async fn execute_work(&self, work: FsWork) -> Result<FsCompletion, DeviceError> {
        self.check_live()?;
        let root = self.root.clone();
        let inodes = Arc::clone(&self.inodes);
        let request = work.request.clone();
        let reply = tokio::task::spawn_blocking(move || execute_request(&root, &inodes, &request))
            .await
            .map_err(|error| DeviceError::Worker(error.to_string()))??;
        Ok(FsCompletion { work, reply })
    }

    async fn complete_work(&self, completion: FsCompletion) -> Result<(), DeviceError> {
        let work = completion.work;
        let used_length = write_reply(&self.resources.dma, &work.reply, &completion.reply)?;
        let queue_layout = self.resources.queues[work.queue_index];
        let queue = VirtQueue::new(queue_layout, &self.resources.dma)?;
        {
            let mut states = self.queue_states.lock().await;
            queue.complete(
                &self.resources.dma,
                &mut states[work.queue_index],
                &work.chain,
                used_length,
            )?;
        }
        let notifier =
            self.resources
                .interrupts
                .get(work.queue_index)
                .ok_or(DeviceError::InvalidLayout(
                    "missing virtio-fs interrupt notifier",
                ))?;
        notifier
            .notify(Interrupt::Queue {
                queue_index: u16::try_from(work.queue_index)
                    .map_err(|_| DeviceError::InvalidLayout("queue index exceeds u16"))?,
                vector: u16::try_from(work.queue_index)
                    .map_err(|_| DeviceError::InvalidLayout("queue vector exceeds u16"))?,
            })
            .await
    }
}

fn parse_chain(
    memory: &DmaMemory,
    chain: &DescriptorChain,
) -> Result<(Vec<u8>, Vec<DmaRange>), DeviceError> {
    let mut request = Vec::new();
    let mut reply = Vec::new();
    for descriptor in &chain.descriptors {
        let range = descriptor_range(descriptor.address, descriptor.length)?;
        if descriptor.flags & VIRTQ_DESC_F_WRITE != 0 {
            reply.push(range);
        } else {
            request.extend_from_slice(&read_range(memory, range)?);
        }
    }
    if request.len() < FUSE_IN_HEADER_SIZE || reply.is_empty() {
        return Err(DeviceError::Descriptor(
            "invalid virtio-fs descriptor directions",
        ));
    }
    Ok((request, reply))
}

fn execute_request(
    root: &Path,
    inodes: &std::sync::Mutex<FsInodes>,
    request: &[u8],
) -> Result<Vec<u8>, DeviceError> {
    let header = FuseInHeader::parse(request)?;
    if usize::try_from(header.length).ok() != Some(request.len()) {
        return Ok(error_reply(header.unique, libc::EINVAL));
    }
    let payload = &request[FUSE_IN_HEADER_SIZE..];
    if matches!(
        header.opcode,
        FUSE_FORGET | FUSE_RELEASE | FUSE_RELEASEDIR | FUSE_DESTROY
    ) {
        return Ok(Vec::new());
    }
    if !matches!(
        header.opcode,
        FUSE_INIT
            | FUSE_LOOKUP
            | FUSE_GETATTR
            | FUSE_OPEN
            | FUSE_OPENDIR
            | FUSE_READ
            | FUSE_READDIR
            | FUSE_STATFS
    ) {
        return Ok(error_reply(header.unique, libc::ENOSYS));
    }
    let result = match header.opcode {
        FUSE_INIT => init_reply(payload),
        FUSE_LOOKUP => lookup_reply(root, inodes, header.node_id, payload),
        FUSE_GETATTR => getattr_reply(inodes, header.node_id),
        FUSE_OPEN | FUSE_OPENDIR => open_reply(inodes, header.node_id),
        FUSE_READ => read_reply(inodes, header.node_id, payload),
        FUSE_READDIR => readdir_reply(root, inodes, header.node_id, payload),
        FUSE_STATFS => Ok(statfs_reply()),
        _ => unreachable!("checked above"),
    };
    match result {
        Ok(body) => Ok(success_reply(header.unique, &body)),
        Err(error) => Ok(error_reply(header.unique, fuse_errno(&error))),
    }
}

fn fuse_errno(error: &DeviceError) -> i32 {
    match error {
        DeviceError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        DeviceError::Descriptor(_) | DeviceError::InvalidLayout(_) => libc::EINVAL,
        DeviceError::DmaRange { .. } | DeviceError::DmaAlignment { .. } => libc::EFAULT,
        DeviceError::Down(_) => libc::EIO,
        DeviceError::InvalidQueue { .. } | DeviceError::Worker(_) => libc::EIO,
    }
}

#[derive(Clone, Copy)]
struct FuseInHeader {
    length: u32,
    opcode: u32,
    unique: u64,
    node_id: u64,
}

impl FuseInHeader {
    fn parse(bytes: &[u8]) -> Result<Self, DeviceError> {
        if bytes.len() < FUSE_IN_HEADER_SIZE {
            return Err(DeviceError::Descriptor("short FUSE request header"));
        }
        Ok(Self {
            length: read_u32(bytes, 0)?,
            opcode: read_u32(bytes, 4)?,
            unique: read_u64(bytes, 8)?,
            node_id: read_u64(bytes, 16)?,
        })
    }
}

fn init_reply(payload: &[u8]) -> Result<Vec<u8>, DeviceError> {
    if payload.len() < 16 {
        return Err(DeviceError::Descriptor("short FUSE_INIT request"));
    }
    let major = read_u32(payload, 0)?;
    if major != FUSE_MIN_VERSION {
        return Err(DeviceError::Descriptor("unsupported FUSE major version"));
    }
    let mut reply = vec![0; FUSE_INIT_OUT_SIZE];
    reply[0..4].copy_from_slice(&FUSE_MIN_VERSION.to_le_bytes());
    reply[4..8].copy_from_slice(&FUSE_MIN_MINOR.to_le_bytes());
    reply[8..12].copy_from_slice(
        &u32::try_from(FUSE_MAX_READ)
            .expect("fixed max read")
            .to_le_bytes(),
    );
    reply[20..24].copy_from_slice(
        &u32::try_from(FUSE_MAX_READ)
            .expect("fixed max read")
            .to_le_bytes(),
    );
    reply[24..28].copy_from_slice(&1u32.to_le_bytes());
    reply[28..30].copy_from_slice(&256u16.to_le_bytes());
    Ok(reply)
}

fn lookup_reply(
    root: &Path,
    inodes: &std::sync::Mutex<FsInodes>,
    parent: u64,
    payload: &[u8],
) -> Result<Vec<u8>, DeviceError> {
    let name = std::str::from_utf8(payload.strip_suffix(&[0]).ok_or(DeviceError::Descriptor(
        "FUSE_LOOKUP name is not NUL terminated",
    ))?)
    .map_err(|_| DeviceError::Descriptor("FUSE_LOOKUP name is not UTF-8"))?;
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(DeviceError::Descriptor("unsafe FUSE_LOOKUP name"));
    }
    let mut guard = inodes
        .lock()
        .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
    let parent_path = guard
        .by_id
        .get(&parent)
        .ok_or(DeviceError::Descriptor("unknown FUSE parent inode"))?;
    let path = parent_path.join(name);
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !path.starts_with(root) {
        return Err(DeviceError::Descriptor(
            "virtio-fs symlink is not supported",
        ));
    }
    let node_id = guard.id_for_path(path)?;
    Ok(entry_out(node_id, &metadata))
}

fn getattr_reply(
    inodes: &std::sync::Mutex<FsInodes>,
    node_id: u64,
) -> Result<Vec<u8>, DeviceError> {
    let guard = inodes
        .lock()
        .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
    let path = guard
        .by_id
        .get(&node_id)
        .ok_or(DeviceError::Descriptor("unknown FUSE inode"))?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(DeviceError::Descriptor(
            "virtio-fs symlink is not supported",
        ));
    }
    let mut result = vec![0; FUSE_ATTR_OUT_SIZE];
    result[..8].copy_from_slice(&1u64.to_le_bytes());
    result[16..].copy_from_slice(&fuse_attr(node_id, &metadata));
    Ok(result)
}

fn open_reply(inodes: &std::sync::Mutex<FsInodes>, node_id: u64) -> Result<Vec<u8>, DeviceError> {
    let guard = inodes
        .lock()
        .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
    let path = guard
        .by_id
        .get(&node_id)
        .ok_or(DeviceError::Descriptor("unknown FUSE inode"))?;
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(DeviceError::Descriptor(
            "virtio-fs symlink is not supported",
        ));
    }
    let mut result = vec![0; FUSE_OPEN_OUT_SIZE];
    result[..8].copy_from_slice(&node_id.to_le_bytes());
    Ok(result)
}

fn read_reply(
    inodes: &std::sync::Mutex<FsInodes>,
    node_id: u64,
    payload: &[u8],
) -> Result<Vec<u8>, DeviceError> {
    if payload.len() < 24 {
        return Err(DeviceError::Descriptor("short FUSE_READ request"));
    }
    let offset = read_u64(payload, 8)?;
    let size = usize::try_from(read_u32(payload, 16)?)
        .map_err(|_| DeviceError::Descriptor("FUSE_READ size overflow"))?;
    let size = size.min(FUSE_MAX_READ);
    let path = {
        let guard = inodes
            .lock()
            .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
        guard
            .by_id
            .get(&node_id)
            .cloned()
            .ok_or(DeviceError::Descriptor("unknown FUSE inode"))?
    };
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut result = vec![0; size];
    let length = file.read(&mut result)?;
    result.truncate(length);
    Ok(result)
}

fn readdir_reply(
    root: &Path,
    inodes: &std::sync::Mutex<FsInodes>,
    node_id: u64,
    payload: &[u8],
) -> Result<Vec<u8>, DeviceError> {
    if payload.len() < 24 {
        return Err(DeviceError::Descriptor("short FUSE_READDIR request"));
    }
    let offset = read_u64(payload, 8)?;
    let maximum = usize::try_from(read_u32(payload, 16)?)
        .map_err(|_| DeviceError::Descriptor("FUSE_READDIR size overflow"))?;
    let path = {
        let guard = inodes
            .lock()
            .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
        guard
            .by_id
            .get(&node_id)
            .cloned()
            .ok_or(DeviceError::Descriptor("unknown FUSE inode"))?
    };
    if !path.starts_with(root) || !path.is_dir() {
        return Err(DeviceError::Descriptor(
            "FUSE_READDIR inode is not a directory",
        ));
    }
    let mut entries = Vec::new();
    let mut index = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if index < offset {
            index += 1;
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            index += 1;
            continue;
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| DeviceError::Descriptor("non-UTF-8 virtio-fs name"))?;
        let mut guard = inodes
            .lock()
            .map_err(|_| DeviceError::Worker("virtio-fs inode lock poisoned".into()))?;
        let id = guard.id_for_path(entry.path())?;
        drop(guard);
        let record = dirent(id, index + 1, &name, &metadata)?;
        if entries
            .len()
            .checked_add(record.len())
            .is_none_or(|length| length > maximum)
        {
            break;
        }
        entries.extend_from_slice(&record);
        index += 1;
    }
    Ok(entries)
}

fn statfs_reply() -> Vec<u8> {
    let mut result = vec![0; FUSE_STATFS_OUT_SIZE];
    result[40..44].copy_from_slice(&4096u32.to_le_bytes());
    result[44..48].copy_from_slice(&255u32.to_le_bytes());
    result[48..52].copy_from_slice(&4096u32.to_le_bytes());
    result
}

fn entry_out(node_id: u64, metadata: &fs::Metadata) -> Vec<u8> {
    let mut result = vec![0; FUSE_ENTRY_OUT_SIZE];
    result[..8].copy_from_slice(&node_id.to_le_bytes());
    result[16..24].copy_from_slice(&1u64.to_le_bytes());
    result[24..32].copy_from_slice(&1u64.to_le_bytes());
    result[40..].copy_from_slice(&fuse_attr(node_id, metadata));
    result
}

fn fuse_attr(node_id: u64, metadata: &fs::Metadata) -> [u8; FUSE_ATTR_SIZE] {
    let mut result = [0u8; FUSE_ATTR_SIZE];
    result[..8].copy_from_slice(&node_id.to_le_bytes());
    result[8..16].copy_from_slice(&metadata.len().to_le_bytes());
    let mode: u32 = if metadata.is_dir() {
        0o040555
    } else {
        0o100444
    };
    result[60..64].copy_from_slice(&mode.to_le_bytes());
    result[64..68].copy_from_slice(&1u32.to_le_bytes());
    result[84..88].copy_from_slice(&4096u32.to_le_bytes());
    result
}

fn dirent(
    node_id: u64,
    offset: u64,
    name: &str,
    metadata: &fs::Metadata,
) -> Result<Vec<u8>, DeviceError> {
    let name_length = u32::try_from(name.len())
        .map_err(|_| DeviceError::Descriptor("directory name too long"))?;
    let raw_length = 24usize
        .checked_add(name.len())
        .ok_or(DeviceError::Descriptor("directory entry overflow"))?;
    let length = raw_length
        .checked_add(7)
        .ok_or(DeviceError::Descriptor("directory entry overflow"))?
        & !7;
    let mut result = vec![0; length];
    result[..8].copy_from_slice(&node_id.to_le_bytes());
    result[8..16].copy_from_slice(&offset.to_le_bytes());
    result[16..20].copy_from_slice(&name_length.to_le_bytes());
    let kind = if metadata.is_dir() { 4u32 } else { 8u32 };
    result[20..24].copy_from_slice(&kind.to_le_bytes());
    result[24..24 + name.len()].copy_from_slice(name.as_bytes());
    Ok(result)
}

fn success_reply(unique: u64, body: &[u8]) -> Vec<u8> {
    let length = FUSE_OUT_HEADER_SIZE
        .checked_add(body.len())
        .expect("FUSE response fits usize");
    let mut reply = Vec::with_capacity(length);
    reply.extend_from_slice(
        &u32::try_from(length)
            .expect("FUSE response fits u32")
            .to_le_bytes(),
    );
    reply.extend_from_slice(&0i32.to_le_bytes());
    reply.extend_from_slice(&unique.to_le_bytes());
    reply.extend_from_slice(body);
    reply
}

fn error_reply(unique: u64, error: i32) -> Vec<u8> {
    let mut reply = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);
    reply.extend_from_slice(&(FUSE_OUT_HEADER_SIZE as u32).to_le_bytes());
    reply.extend_from_slice(&(-error).to_le_bytes());
    reply.extend_from_slice(&unique.to_le_bytes());
    reply
}

fn descriptor_range(address: u64, length: u32) -> Result<DmaRange, DeviceError> {
    if length == 0 {
        return Err(DeviceError::Descriptor("zero-length virtio-fs descriptor"));
    }
    Ok(DmaRange::new(
        address,
        usize::try_from(length).expect("u32 fits usize"),
    ))
}

fn read_range(memory: &DmaMemory, range: DmaRange) -> Result<Vec<u8>, DeviceError> {
    let lease = memory.lease(range)?;
    let mut bytes = Vec::with_capacity(range.length);
    for part in lease.parts() {
        bytes.extend_from_slice(unsafe { part.read_slice() });
    }
    Ok(bytes)
}

fn write_reply(memory: &DmaMemory, ranges: &[DmaRange], reply: &[u8]) -> Result<u32, DeviceError> {
    if reply.is_empty() {
        return Ok(0);
    }
    let capacity = ranges
        .iter()
        .try_fold(0usize, |total, range| total.checked_add(range.length))
        .ok_or(DeviceError::Descriptor("virtio-fs reply capacity overflow"))?;
    if reply.len() > capacity {
        return Err(DeviceError::Descriptor(
            "virtio-fs reply does not fit descriptors",
        ));
    }
    let mut copied = 0usize;
    for range in ranges {
        let mut lease = memory.lease(*range)?;
        for part in lease.parts_mut() {
            let target = unsafe { part.write_slice() };
            let remaining = reply.len() - copied;
            let length = remaining.min(target.len());
            target[..length].copy_from_slice(&reply[copied..copied + length]);
            copied += length;
            if copied == reply.len() {
                return u32::try_from(copied)
                    .map_err(|_| DeviceError::Descriptor("virtio-fs reply too large"));
            }
        }
    }
    Err(DeviceError::Descriptor("short virtio-fs reply write"))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, DeviceError> {
    let end = offset
        .checked_add(4)
        .ok_or(DeviceError::Descriptor("FUSE field overflow"))?;
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or(DeviceError::Descriptor("short FUSE field"))?
            .try_into()
            .expect("four bytes"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, DeviceError> {
    let end = offset
        .checked_add(8)
        .ok_or(DeviceError::Descriptor("FUSE field overflow"))?;
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..end)
            .ok_or(DeviceError::Descriptor("short FUSE field"))?
            .try_into()
            .expect("eight bytes"),
    ))
}

#[async_trait]
impl DeviceDeclaration for FsDeclaration {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: self.request_queue_count + 1,
            maximum_queue_size: self.maximum_queue_size,
            notifier_count: self.request_queue_count + 1,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: 0,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Arc<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        Ok(Arc::new(FsDevice::new(self, resources)))
    }
}

#[async_trait]
impl DeviceInstance for FsDevice {
    fn kick(&self) {
        self.wake.notify_one();
    }

    fn stop(&self, _reason: DeviceDownReason) {
        self.down.store(true, Ordering::Release);
        self.resources.dma.revoke();
        self.wake.notify_waiters();
    }

    async fn run(&self) -> Result<(), DeviceError> {
        let maximum_inflight = self
            .resources
            .queues
            .len()
            .saturating_mul(MAXIMUM_INFLIGHT_PER_QUEUE)
            .clamp(1, MAXIMUM_INFLIGHT_REQUESTS);
        let mut pending = FuturesUnordered::new();
        let mut next_queue = 0usize;
        let mut scan_queues = false;
        loop {
            if !scan_queues && pending.is_empty() && !self.down.load(Ordering::Acquire) {
                let notified = self.wake.notified();
                if !self.down.load(Ordering::Acquire) {
                    notified.await;
                    scan_queues = true;
                }
            }
            while !self.down.load(Ordering::Acquire)
                && scan_queues
                && pending.len() < maximum_inflight
            {
                let mut work = None;
                for _ in 0..self.resources.queues.len() {
                    let queue_index = next_queue % self.resources.queues.len();
                    next_queue = next_queue.wrapping_add(1);
                    if let Some(next) = self.take_work(queue_index).await? {
                        work = Some(next);
                        break;
                    }
                }
                let Some(work) = work else {
                    break;
                };
                pending.push(self.execute_work(work));
            }
            scan_queues = false;
            if self.down.load(Ordering::Acquire) && pending.is_empty() {
                self.resources.dma.wait_for_drain().await;
                return Ok(());
            }
            if pending.is_empty() {
                continue;
            }
            tokio::select! {
                result = pending.next() => {
                    let completion = result.expect("pending virtio-fs completion exists")?;
                    if !self.down.load(Ordering::Acquire) {
                        self.complete_work(completion).await?;
                        scan_queues = true;
                    }
                }
                _ = self.wake.notified() => { scan_queues = true; }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::device::DeviceDeclaration;

    use super::{FUSE_IN_HEADER_SIZE, FUSE_INIT, FsDeclaration, execute_request};

    fn root() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("libvirtiod-fs-{unique}"));
        std::fs::create_dir(&root).expect("create root");
        root
    }

    #[test]
    fn declaration_exports_standard_config() {
        let root = root();
        let declaration = FsDeclaration::open(&root, "shared", 2, 128).expect("declaration");
        assert_eq!(declaration.config_bytes()[..6], *b"shared");
        assert_eq!(
            u32::from_le_bytes(
                declaration.config_bytes()[36..40]
                    .try_into()
                    .expect("queue count")
            ),
            2
        );
        assert_eq!(declaration.layout().queue_count, 3);
        let entry = root.join("entry");
        std::fs::write(&entry, b"data").expect("create entry");
        let reply = super::entry_out(9, &std::fs::metadata(&entry).expect("entry metadata"));
        assert_eq!(
            u64::from_le_bytes(reply[40..48].try_into().expect("attr inode")),
            9
        );
        std::fs::remove_file(entry).expect("remove entry");
        std::fs::remove_dir(root).expect("remove root");
    }

    #[test]
    fn init_request_returns_a_fuse_reply() {
        let root = root();
        let mut request = vec![0; FUSE_IN_HEADER_SIZE + 16];
        let length = u32::try_from(request.len()).expect("length");
        request[0..4].copy_from_slice(&length.to_le_bytes());
        request[4..8].copy_from_slice(&FUSE_INIT.to_le_bytes());
        request[8..16].copy_from_slice(&17u64.to_le_bytes());
        request[16..24].copy_from_slice(&1u64.to_le_bytes());
        request[FUSE_IN_HEADER_SIZE..FUSE_IN_HEADER_SIZE + 4].copy_from_slice(&7u32.to_le_bytes());
        let reply = execute_request(
            &root,
            &std::sync::Mutex::new(super::FsInodes::new(root.clone())),
            &request,
        )
        .expect("init reply");
        assert_eq!(
            u32::from_le_bytes(reply[0..4].try_into().expect("length")),
            80
        );
        assert_eq!(
            i32::from_le_bytes(reply[4..8].try_into().expect("error")),
            0
        );
        assert_eq!(
            u64::from_le_bytes(reply[8..16].try_into().expect("unique")),
            17
        );
        assert_eq!(
            u32::from_le_bytes(reply[16 + 24..16 + 28].try_into().expect("time gran")),
            1
        );
        assert_eq!(
            u16::from_le_bytes(reply[16 + 28..16 + 30].try_into().expect("max pages")),
            256
        );
        std::fs::remove_dir(root).expect("remove root");
    }
}
