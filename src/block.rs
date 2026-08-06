use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
use crate::dma::{DmaLease, DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_BLK_F_FLUSH: u64 = 1 << 9;
pub const VIRTIO_BLK_T_IN: u32 = 0;
pub const VIRTIO_BLK_T_OUT: u32 = 1;
pub const VIRTIO_BLK_T_FLUSH: u32 = 4;
pub const VIRTIO_BLK_S_OK: u8 = 0;
pub const VIRTIO_BLK_S_IOERR: u8 = 1;
pub const VIRTIO_BLK_S_UNSUPP: u8 = 2;

const VIRTQ_DESC_F_WRITE: u16 = 2;
const VIRTIO_BLK_HEADER_SIZE: usize = 16;
const VIRTIO_BLK_SECTOR_SIZE: u64 = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockRequestType {
    In,
    Out,
    Flush,
    Unsupported(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockRequest {
    pub request_type: BlockRequestType,
    pub sector: u64,
    pub payload: Vec<DmaRange>,
    pub status: DmaRange,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockCompletion {
    pub status: u8,
    pub used_length: u32,
}

#[derive(Clone, Debug)]
pub struct BlockDeclaration {
    own_imm_path: PathBuf,
    imm_queue_count: usize,
    imm_maximum_queue_size: u16,
    imm_read_only: bool,
}

impl BlockDeclaration {
    pub fn new(path: impl Into<PathBuf>, queue_count: usize, maximum_queue_size: u16) -> Self {
        Self {
            own_imm_path: path.into(),
            imm_queue_count: queue_count,
            imm_maximum_queue_size: maximum_queue_size,
            imm_read_only: false,
        }
    }

    pub fn read_only(mut self) -> Self {
        self.imm_read_only = true;
        self
    }

    pub fn path(&self) -> &Path {
        &self.own_imm_path
    }
}

pub struct BlockDevice {
    own_imm_file: File,
    own_imm_resources: DeviceResources,
    imm_read_only: bool,
    imm_capacity: u64,
    own_mut_queue_states: Mutex<Vec<QueueState>>,
    atomic_mut_kicked: AtomicBool,
    atomic_mut_down: AtomicBool,
}

impl BlockDevice {
    fn open(
        declaration: &BlockDeclaration,
        resources: DeviceResources,
    ) -> Result<Self, DeviceError> {
        let file = OpenOptions::new()
            .read(true)
            .write(!declaration.imm_read_only)
            .open(&declaration.own_imm_path)?;
        let capacity = file.metadata()?.len();
        if capacity == 0 || capacity % VIRTIO_BLK_SECTOR_SIZE != 0 {
            return Err(DeviceError::InvalidLayout(
                "block image must have a non-zero sector-aligned size",
            ));
        }
        let queue_count = resources.queues.len();
        Ok(Self {
            own_imm_file: file,
            own_imm_resources: resources,
            imm_read_only: declaration.imm_read_only,
            imm_capacity: capacity,
            own_mut_queue_states: Mutex::new(vec![QueueState::new(); queue_count]),
            atomic_mut_kicked: AtomicBool::new(false),
            atomic_mut_down: AtomicBool::new(false),
        })
    }

    pub fn resources(&self) -> &DeviceResources {
        &self.own_imm_resources
    }

    pub fn take_kick(&self) -> bool {
        self.atomic_mut_kicked.swap(false, Ordering::AcqRel)
    }

    pub fn parse_request(
        memory: &DmaMemory,
        chain: &DescriptorChain,
    ) -> Result<BlockRequest, DeviceError> {
        if chain.descriptors.len() < 2 {
            return Err(DeviceError::Descriptor(
                "block request has too few descriptors",
            ));
        }
        let header = chain.descriptors.first().expect("checked above");
        let status = chain.descriptors.last().expect("checked above");
        if header.flags & VIRTQ_DESC_F_WRITE != 0 || header.length != VIRTIO_BLK_HEADER_SIZE as u32
        {
            return Err(DeviceError::Descriptor("invalid block request header"));
        }
        if status.flags & VIRTQ_DESC_F_WRITE == 0 || status.length != 1 {
            return Err(DeviceError::Descriptor("invalid block request status byte"));
        }

        let header_range = descriptor_range(header.address, header.length)?;
        let status_range = descriptor_range(status.address, status.length)?;
        let header_lease = memory.lease(header_range)?;
        let mut header_bytes = [0u8; VIRTIO_BLK_HEADER_SIZE];
        copy_from_lease(&header_lease, &mut header_bytes)?;
        drop(header_lease);

        let request_code = u32::from_le_bytes(header_bytes[0..4].try_into().expect("type"));
        let request_type = match request_code {
            VIRTIO_BLK_T_IN => BlockRequestType::In,
            VIRTIO_BLK_T_OUT => BlockRequestType::Out,
            VIRTIO_BLK_T_FLUSH => BlockRequestType::Flush,
            value => BlockRequestType::Unsupported(value),
        };
        let sector = u64::from_le_bytes(header_bytes[8..16].try_into().expect("sector"));
        let mut payload = Vec::with_capacity(chain.descriptors.len() - 2);
        for descriptor in &chain.descriptors[1..chain.descriptors.len() - 1] {
            let writable = descriptor.flags & VIRTQ_DESC_F_WRITE != 0;
            match request_type {
                BlockRequestType::In if !writable => {
                    return Err(DeviceError::Descriptor(
                        "block read payload is not writable",
                    ));
                }
                BlockRequestType::Out if writable => {
                    return Err(DeviceError::Descriptor("block write payload is writable"));
                }
                BlockRequestType::Flush => {
                    return Err(DeviceError::Descriptor("block flush has payload"));
                }
                _ => {}
            }
            let range = descriptor_range(descriptor.address, descriptor.length)?;
            memory.check_range(range)?;
            payload.push(range);
        }
        if matches!(request_type, BlockRequestType::In | BlockRequestType::Out)
            && payload.is_empty()
        {
            return Err(DeviceError::Descriptor("block request has no payload"));
        }
        memory.check_range(status_range)?;
        Ok(BlockRequest {
            request_type,
            sector,
            payload,
            status: status_range,
        })
    }

    pub async fn execute(
        &self,
        memory: &DmaMemory,
        request: BlockRequest,
    ) -> Result<BlockCompletion, DeviceError> {
        self.check_live()?;
        let mut payload = Vec::with_capacity(request.payload.len());
        for range in &request.payload {
            payload.push(memory.lease(*range)?);
        }
        let status = memory.lease(request.status)?;
        let file = self.own_imm_file.try_clone()?;
        let capacity = self.imm_capacity;
        let read_only = self.imm_read_only;
        tokio::task::spawn_blocking(move || {
            let (completion, status_value) =
                execute_blocking(file, capacity, read_only, request, payload)?;
            write_status(status, status_value)?;
            Ok(completion)
        })
        .await
        .map_err(|error| DeviceError::Worker(error.to_string()))?
    }

    fn check_live(&self) -> Result<(), DeviceError> {
        if self.atomic_mut_down.load(Ordering::Acquire) {
            return Err(DeviceError::Down(DeviceDownReason::Revoked));
        }
        Ok(())
    }

    async fn process_queue(&self, queue_index: usize) -> Result<bool, DeviceError> {
        let queue_layout = *self
            .own_imm_resources
            .queues
            .get(queue_index)
            .ok_or(DeviceError::InvalidLayout("queue index is not configured"))?;
        let queue = VirtQueue::new(queue_layout, &self.own_imm_resources.dma)?;
        let chain = {
            let mut states = self.own_mut_queue_states.lock().await;
            queue.pop(&self.own_imm_resources.dma, &mut states[queue_index])?
        };
        let Some(chain) = chain else {
            return Ok(false);
        };
        let request = Self::parse_request(&self.own_imm_resources.dma, &chain)?;
        let completion = self.execute(&self.own_imm_resources.dma, request).await?;
        {
            let mut states = self.own_mut_queue_states.lock().await;
            queue.complete(
                &self.own_imm_resources.dma,
                &mut states[queue_index],
                &chain,
                completion.used_length,
            )?;
        }
        let notifier = self.own_imm_resources.interrupts.get(queue_index).ok_or(
            DeviceError::InvalidLayout("missing queue interrupt notifier"),
        )?;
        notifier
            .notify(crate::interrupt::Interrupt::Queue {
                queue_index: u16::try_from(queue_index)
                    .map_err(|_| DeviceError::InvalidLayout("queue index exceeds u16"))?,
                vector: u16::try_from(queue_index)
                    .map_err(|_| DeviceError::InvalidLayout("vector exceeds u16"))?,
            })
            .await?;
        Ok(true)
    }
}

fn descriptor_range(address: u64, length: u32) -> Result<DmaRange, DeviceError> {
    if length == 0 {
        return Err(DeviceError::Descriptor("zero-length descriptor"));
    }
    Ok(DmaRange::new(
        address,
        usize::try_from(length).expect("u32 fits usize"),
    ))
}

fn copy_from_lease(lease: &DmaLease, target: &mut [u8]) -> Result<(), DeviceError> {
    let mut offset = 0usize;
    for part in lease.parts() {
        let bytes = unsafe { part.read_slice() };
        let end = offset
            .checked_add(bytes.len())
            .ok_or(DeviceError::Descriptor("header overflow"))?;
        if end > target.len() {
            return Err(DeviceError::Descriptor("block header has invalid length"));
        }
        target[offset..end].copy_from_slice(bytes);
        offset = end;
    }
    if offset != target.len() {
        return Err(DeviceError::Descriptor("block header is truncated"));
    }
    Ok(())
}

fn execute_blocking(
    file: File,
    capacity: u64,
    read_only: bool,
    request: BlockRequest,
    mut payload: Vec<DmaLease>,
) -> Result<(BlockCompletion, u8), DeviceError> {
    let status = match request.request_type {
        BlockRequestType::Flush => {
            if !payload.is_empty() {
                return Err(DeviceError::Descriptor("block flush has payload"));
            }
            if file.sync_data().is_ok() {
                VIRTIO_BLK_S_OK
            } else {
                VIRTIO_BLK_S_IOERR
            }
        }
        BlockRequestType::Unsupported(_) => VIRTIO_BLK_S_UNSUPP,
        BlockRequestType::In | BlockRequestType::Out => {
            let offset = request
                .sector
                .checked_mul(VIRTIO_BLK_SECTOR_SIZE)
                .ok_or(DeviceError::Descriptor("block sector overflow"))?;
            let length = payload.iter().try_fold(0u64, |total, lease| {
                lease.parts().iter().try_fold(total, |sum, part| {
                    sum.checked_add(u64::try_from(part.length()).expect("usize fits u64"))
                        .ok_or(DeviceError::Descriptor("block request length overflow"))
                })
            })?;
            if offset > capacity
                || length > capacity - offset
                || (request.request_type == BlockRequestType::Out && read_only)
            {
                VIRTIO_BLK_S_IOERR
            } else {
                let mut iovecs = Vec::new();
                for lease in &mut payload {
                    for part in lease.parts_mut() {
                        iovecs.push(libc::iovec {
                            iov_base: unsafe { part.as_ptr().cast() },
                            iov_len: part.length(),
                        });
                    }
                }
                let result = unsafe {
                    match request.request_type {
                        BlockRequestType::In => libc::preadv(
                            std::os::fd::AsRawFd::as_raw_fd(&file),
                            iovecs.as_mut_ptr(),
                            i32::try_from(iovecs.len()).map_err(|_| {
                                DeviceError::Descriptor("too many block I/O vectors")
                            })?,
                            i64::try_from(offset).map_err(|_| {
                                DeviceError::Descriptor("block offset exceeds host off_t")
                            })?,
                        ),
                        BlockRequestType::Out => libc::pwritev(
                            std::os::fd::AsRawFd::as_raw_fd(&file),
                            iovecs.as_ptr(),
                            i32::try_from(iovecs.len()).map_err(|_| {
                                DeviceError::Descriptor("too many block I/O vectors")
                            })?,
                            i64::try_from(offset).map_err(|_| {
                                DeviceError::Descriptor("block offset exceeds host off_t")
                            })?,
                        ),
                        _ => unreachable!(),
                    }
                };
                if result < 0 || u64::try_from(result).expect("non-negative") != length {
                    VIRTIO_BLK_S_IOERR
                } else {
                    VIRTIO_BLK_S_OK
                }
            }
        }
    };
    let used_length = if status == VIRTIO_BLK_S_OK && request.request_type == BlockRequestType::In {
        payload
            .iter()
            .flat_map(|lease| lease.parts())
            .try_fold(1u32, |total, part| {
                total
                    .checked_add(
                        u32::try_from(part.length())
                            .map_err(|_| DeviceError::Descriptor("used length overflow"))?,
                    )
                    .ok_or(DeviceError::Descriptor("used length overflow"))
            })?
    } else {
        1
    };
    Ok((
        BlockCompletion {
            status,
            used_length,
        },
        status,
    ))
}

fn write_status(mut lease: DmaLease, status: u8) -> Result<(), DeviceError> {
    if lease.parts().len() != 1 || lease.parts()[0].length() != 1 {
        return Err(DeviceError::Descriptor("block status lease is malformed"));
    }
    let bytes = unsafe { lease.parts_mut()[0].write_slice() };
    bytes[0] = status;
    Ok(())
}

#[async_trait]
impl DeviceDeclaration for BlockDeclaration {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: self.imm_queue_count,
            maximum_queue_size: self.imm_maximum_queue_size,
            notifier_count: self.imm_queue_count,
            required_features: VIRTIO_BLK_F_FLUSH,
            optional_features: 0,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Box<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        Ok(Box::new(BlockDevice::open(self, resources)?))
    }
}

#[async_trait]
impl DeviceInstance for BlockDevice {
    fn kick(&self) {
        self.atomic_mut_kicked.store(true, Ordering::Release);
    }

    async fn process_kick(&self) -> Result<(), DeviceError> {
        if !self.take_kick() {
            return Ok(());
        }
        loop {
            let mut did_work = false;
            for queue_index in 0..self.own_imm_resources.queues.len() {
                did_work |= self.process_queue(queue_index).await?;
            }
            if !did_work {
                return Ok(());
            }
        }
    }

    async fn shutdown(&self, _reason: DeviceDownReason) -> Result<(), DeviceError> {
        self.atomic_mut_down.store(true, Ordering::Release);
        self.own_imm_resources.dma.revoke();
        self.own_imm_resources.dma.wait_for_drain().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        BlockDeclaration, BlockDevice, BlockRequestType, VIRTIO_BLK_F_FLUSH, VIRTIO_BLK_S_OK,
    };
    use crate::device::{DeviceInstance, DeviceResources};
    use crate::dma::{DmaMemory, DmaRange, DmaSegment};
    use crate::error::{DeviceDownReason, DeviceError};
    use crate::interrupt::{Interrupt, InterruptNotifier};
    use crate::queue::{QueueLayout, VirtQueue};

    #[derive(Default)]
    struct TestNotifier {
        atomic_mut_count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl InterruptNotifier for TestNotifier {
        async fn notify(&self, interrupt: Interrupt) -> Result<(), DeviceError> {
            assert_eq!(
                interrupt,
                Interrupt::Queue {
                    queue_index: 0,
                    vector: 0
                }
            );
            self.atomic_mut_count.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    fn image_path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("libvirtiod-block-{unique}.raw"))
    }

    fn resources(memory: &mut [u8], notifier: Arc<dyn InterruptNotifier>) -> DeviceResources {
        DeviceResources {
            queues: vec![QueueLayout {
                index: 0,
                size: 8,
                descriptors: DmaRange::new(0x1000, 8 * 16),
                available: DmaRange::new(0x1080, 4 + 8 * 2),
                used: DmaRange::new(0x10a0, 4 + 8 * 8),
            }],
            dma: DmaMemory::new(
                1,
                vec![unsafe {
                    DmaSegment::new(0x1000, NonNull::from(&mut memory[0]), memory.len())
                }],
            )
            .expect("DMA memory"),
            interrupts: vec![notifier],
            negotiated_features: VIRTIO_BLK_F_FLUSH,
        }
    }

    #[tokio::test]
    async fn executes_a_zero_copy_read_request() {
        let path = image_path();
        let mut image = vec![0u8; 4096];
        image[512..516].copy_from_slice(b"vmm\n");
        fs::write(&path, image).expect("create image");

        let mut memory = [0u8; 4096];
        memory[0..8].copy_from_slice(&0x1200u64.to_le_bytes());
        memory[8..12].copy_from_slice(&(16u32).to_le_bytes());
        memory[12..14].copy_from_slice(&(1u16).to_le_bytes());
        memory[14..16].copy_from_slice(&(1u16).to_le_bytes());
        memory[16..24].copy_from_slice(&0x1300u64.to_le_bytes());
        memory[24..28].copy_from_slice(&(4u32).to_le_bytes());
        memory[28..30].copy_from_slice(&(3u16).to_le_bytes());
        memory[30..32].copy_from_slice(&(2u16).to_le_bytes());
        memory[32..40].copy_from_slice(&0x1400u64.to_le_bytes());
        memory[40..44].copy_from_slice(&(1u32).to_le_bytes());
        memory[44..46].copy_from_slice(&(2u16).to_le_bytes());
        memory[0x200..0x204].copy_from_slice(&(0u32).to_le_bytes());
        memory[0x208..0x210].copy_from_slice(&(1u64).to_le_bytes());

        let declaration = BlockDeclaration::new(&path, 1, 128);
        let device = BlockDevice::open(
            &declaration,
            resources(&mut memory, Arc::new(TestNotifier::default())),
        )
        .expect("open image");
        let queue =
            VirtQueue::new(device.resources().queues[0], &device.resources().dma).expect("queue");
        let chain = unsafe { queue.read_chain(&device.resources().dma, 0) }.expect("chain");
        let request = BlockDevice::parse_request(&device.resources().dma, &chain).expect("request");
        assert_eq!(request.request_type, BlockRequestType::In);

        let completion = device
            .execute(&device.resources().dma, request)
            .await
            .expect("execute");
        assert_eq!(completion.status, VIRTIO_BLK_S_OK);
        assert_eq!(completion.used_length, 5);
        let payload = device
            .resources()
            .dma
            .lease(DmaRange::new(0x1300, 4))
            .expect("payload");
        assert_eq!(unsafe { payload.parts()[0].read_slice() }, b"vmm\n");
        let status = device
            .resources()
            .dma
            .lease(DmaRange::new(0x1400, 1))
            .expect("status");
        assert_eq!(
            unsafe { status.parts()[0].read_slice() },
            &[VIRTIO_BLK_S_OK]
        );
        drop(status);
        drop(payload);
        device
            .shutdown(DeviceDownReason::Stop)
            .await
            .expect("shutdown");
        fs::remove_file(path).expect("remove image");
    }

    #[tokio::test]
    async fn processes_avail_ring_and_notifies_completion() {
        let path = image_path();
        let mut image = vec![0u8; 4096];
        image[512..516].copy_from_slice(b"vmm\n");
        fs::write(&path, image).expect("create image");

        let mut memory = [0u8; 4096];
        memory[0..8].copy_from_slice(&0x1200u64.to_le_bytes());
        memory[8..12].copy_from_slice(&(16u32).to_le_bytes());
        memory[12..14].copy_from_slice(&(1u16).to_le_bytes());
        memory[14..16].copy_from_slice(&(1u16).to_le_bytes());
        memory[16..24].copy_from_slice(&0x1300u64.to_le_bytes());
        memory[24..28].copy_from_slice(&(4u32).to_le_bytes());
        memory[28..30].copy_from_slice(&(3u16).to_le_bytes());
        memory[30..32].copy_from_slice(&(2u16).to_le_bytes());
        memory[32..40].copy_from_slice(&0x1400u64.to_le_bytes());
        memory[40..44].copy_from_slice(&(1u32).to_le_bytes());
        memory[44..46].copy_from_slice(&(2u16).to_le_bytes());
        memory[0x200..0x204].copy_from_slice(&(0u32).to_le_bytes());
        memory[0x208..0x210].copy_from_slice(&(1u64).to_le_bytes());
        memory[0x82..0x84].copy_from_slice(&1u16.to_le_bytes());
        memory[0x84..0x86].copy_from_slice(&0u16.to_le_bytes());

        let notifier = Arc::new(TestNotifier::default());
        let declaration = BlockDeclaration::new(&path, 1, 128);
        let device = BlockDevice::open(&declaration, resources(&mut memory, notifier.clone()))
            .expect("open image");
        device.kick();
        device.process_kick().await.expect("process kick");

        let used = device
            .resources()
            .dma
            .lease(DmaRange::new(0x10a0, 12))
            .expect("used ring");
        let bytes = unsafe { used.parts()[0].read_slice() };
        assert_eq!(
            u16::from_le_bytes(bytes[2..4].try_into().expect("used index")),
            1
        );
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().expect("used head")),
            0
        );
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().expect("used length")),
            5
        );
        drop(used);
        let payload = device
            .resources()
            .dma
            .lease(DmaRange::new(0x1300, 4))
            .expect("payload");
        assert_eq!(unsafe { payload.parts()[0].read_slice() }, b"vmm\n");
        drop(payload);
        let status = device
            .resources()
            .dma
            .lease(DmaRange::new(0x1400, 1))
            .expect("status");
        assert_eq!(
            unsafe { status.parts()[0].read_slice() },
            &[VIRTIO_BLK_S_OK]
        );
        drop(status);
        assert_eq!(notifier.atomic_mut_count.load(Ordering::Acquire), 1);
        device
            .shutdown(DeviceDownReason::Stop)
            .await
            .expect("shutdown");
        fs::remove_file(path).expect("remove image");
    }
}
