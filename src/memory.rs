//! Virtio-mem protocol model and host-memory backend boundary.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, watch};

use crate::device::{DeviceConfig, DeviceInstance, DeviceLayout, DeviceResources, DeviceSpec};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;
pub const VIRTIO_MEM_F_ACPI_PXM: u64 = 1;
pub const VIRTIO_MEM_F_UNPLUGGED_INACCESSIBLE: u64 = 1 << 1;
pub const VIRTIO_MEM_REQ_PLUG: u16 = 0;
pub const VIRTIO_MEM_REQ_UNPLUG: u16 = 1;
pub const VIRTIO_MEM_REQ_UNPLUG_ALL: u16 = 2;
pub const VIRTIO_MEM_REQ_STATE: u16 = 3;
pub const VIRTIO_MEM_RESP_ACK: u16 = 0;
pub const VIRTIO_MEM_RESP_NACK: u16 = 1;
pub const VIRTIO_MEM_RESP_BUSY: u16 = 2;
pub const VIRTIO_MEM_RESP_ERROR: u16 = 3;
pub const VIRTIO_MEM_STATE_PLUGGED: u16 = 0;
pub const VIRTIO_MEM_STATE_UNPLUGGED: u16 = 1;
pub const VIRTIO_MEM_STATE_MIXED: u16 = 2;

const REQUEST_QUEUE: usize = 0;
const CONFIG_SIZE: usize = 56;
const REQUEST_SIZE: usize = 24;
const RESPONSE_SIZE: usize = 10;
const WRITE_FLAG: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryConfig {
    pub block_size: u64,
    pub node_id: u16,
    pub addr: u64,
    pub region_size: u64,
    pub usable_region_size: u64,
    pub plugged_size: u64,
    pub requested_size: u64,
}

impl MemoryConfig {
    fn validate(self) -> Result<Self, DeviceError> {
        if self.block_size == 0 || !self.block_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout(
                "virtio-mem block size is not a power of two",
            ));
        }
        for value in [
            self.addr,
            self.region_size,
            self.usable_region_size,
            self.plugged_size,
            self.requested_size,
        ] {
            if value % self.block_size != 0 {
                return Err(DeviceError::InvalidLayout(
                    "virtio-mem value is not block aligned",
                ));
            }
        }
        if self.usable_region_size > self.region_size
            || self.plugged_size > self.usable_region_size
            || self.requested_size > self.usable_region_size
        {
            return Err(DeviceError::InvalidLayout(
                "invalid virtio-mem region sizes",
            ));
        }
        Ok(self)
    }

    fn encode(self) -> [u8; CONFIG_SIZE] {
        let mut bytes = [0; CONFIG_SIZE];
        bytes[0..8].copy_from_slice(&self.block_size.to_le_bytes());
        bytes[8..10].copy_from_slice(&self.node_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.addr.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.region_size.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.usable_region_size.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.plugged_size.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.requested_size.to_le_bytes());
        bytes
    }
}

#[derive(Clone)]
pub struct MemoryConfigState {
    config: Arc<RwLock<MemoryConfig>>,
    updates: watch::Sender<u64>,
}

impl MemoryConfigState {
    pub fn new(config: MemoryConfig) -> Result<Self, DeviceError> {
        config.validate()?;
        let (updates, _) = watch::channel(0);
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            updates,
        })
    }

    pub fn snapshot(&self) -> MemoryConfig {
        *self.config.read().expect("memory config lock poisoned")
    }

    pub fn update(&self, config: MemoryConfig) -> Result<(), DeviceError> {
        config.validate()?;
        *self.config.write().expect("memory config lock poisoned") = config;
        let generation = *self.updates.borrow() + 1;
        let _ = self.updates.send(generation);
        Ok(())
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.updates.subscribe()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryRequest {
    Plug { addr: u64, blocks: u16 },
    Unplug { addr: u64, blocks: u16 },
    UnplugAll,
    State { addr: u64, blocks: u16 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryResponse {
    Ack,
    Nack,
    Busy,
    Error,
    State(u16),
}

#[async_trait]
pub trait MemoryBackend: Send + Sync {
    fn config_state(&self) -> MemoryConfigState;
    async fn handle(&self, request: MemoryRequest) -> Result<MemoryResponse, DeviceError>;
    fn shutdown(&self);
}

pub struct MemorySpec {
    maximum_queue_size: u16,
    backend: Arc<dyn MemoryBackend>,
    optional_features: u64,
}

impl MemorySpec {
    pub fn new(
        maximum_queue_size: u16,
        backend: Arc<dyn MemoryBackend>,
    ) -> Result<Self, DeviceError> {
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout("invalid virtio-mem queue size"));
        }
        Ok(Self {
            maximum_queue_size,
            backend,
            optional_features: VIRTIO_MEM_F_UNPLUGGED_INACCESSIBLE,
        })
    }

    pub fn with_acpi_pxm(mut self) -> Self {
        self.optional_features |= VIRTIO_MEM_F_ACPI_PXM;
        self
    }
}

struct ConfigView {
    state: MemoryConfigState,
}

impl DeviceConfig for ConfigView {
    fn size(&self) -> usize {
        CONFIG_SIZE
    }

    fn read(&self, offset: usize, bytes: &mut [u8]) -> Result<(), DeviceError> {
        let config = self.state.snapshot().encode();
        let end = offset
            .checked_add(bytes.len())
            .ok_or(DeviceError::InvalidLayout(
                "virtio-mem config offset overflow",
            ))?;
        if end > CONFIG_SIZE {
            return Err(DeviceError::InvalidLayout(
                "virtio-mem config read exceeds size",
            ));
        }
        bytes.copy_from_slice(&config[offset..end]);
        Ok(())
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }
}

struct MemoryDevice {
    resources: DeviceResources,
    backend: Arc<dyn MemoryBackend>,
    config: Arc<dyn DeviceConfig>,
    queue_state: Mutex<QueueState>,
    wake: Notify,
    down: AtomicBool,
}

#[async_trait]
impl DeviceSpec for MemorySpec {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: 1,
            maximum_queue_size: self.maximum_queue_size,
            notifier_count: 2,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: self.optional_features,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Arc<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        let state = self.backend.config_state();
        state.snapshot().validate()?;
        Ok(Arc::new(MemoryDevice {
            resources,
            backend: Arc::clone(&self.backend),
            config: Arc::new(ConfigView { state }),
            queue_state: Mutex::new(QueueState::new()),
            wake: Notify::new(),
            down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for MemoryDevice {
    fn kick(&self) {
        self.wake.notify_one();
    }
    fn stop(&self, _reason: DeviceDownReason) {
        self.down.store(true, Ordering::Release);
        self.backend.shutdown();
        self.resources.dma.revoke();
        self.wake.notify_waiters();
    }
    fn config(&self) -> Option<Arc<dyn DeviceConfig>> {
        Some(Arc::clone(&self.config))
    }

    async fn run(&self) -> Result<(), DeviceError> {
        loop {
            let notified = self.wake.notified();
            if self.down.load(Ordering::Acquire) {
                self.resources.dma.wait_for_drain().await;
                return Ok(());
            }
            notified.await;
            if self.down.load(Ordering::Acquire) {
                self.resources.dma.wait_for_drain().await;
                return Ok(());
            }
            while self.process_request().await? {}
        }
    }
}

impl MemoryDevice {
    async fn process_request(&self) -> Result<bool, DeviceError> {
        let queue = VirtQueue::new(self.resources.queues[REQUEST_QUEUE], &self.resources.dma)?;
        let chain = {
            let mut state = self.queue_state.lock().await;
            queue.pop(&self.resources.dma, &mut state)?
        };
        let Some(chain) = chain else { return Ok(false) };
        let request = parse_request(&self.resources.dma, &chain)?;
        let response = self.backend.handle(request).await?;
        write_response(&self.resources.dma, &chain, response)?;
        {
            let mut state = self.queue_state.lock().await;
            queue.complete(
                &self.resources.dma,
                &mut state,
                &chain,
                RESPONSE_SIZE as u32,
            )?;
        }
        self.resources.interrupts[REQUEST_QUEUE]
            .notify(Interrupt::Queue {
                queue_index: 0,
                vector: 0,
            })
            .await?;
        Ok(true)
    }
}

fn parse_request(
    memory: &DmaMemory,
    chain: &DescriptorChain,
) -> Result<MemoryRequest, DeviceError> {
    if chain.descriptors.len() != 2 {
        return Err(DeviceError::Descriptor(
            "virtio-mem request must have two descriptors",
        ));
    }
    let request = chain.descriptors[0];
    let response = chain.descriptors[1];
    if request.flags & WRITE_FLAG != 0 || request.length != REQUEST_SIZE as u32 {
        return Err(DeviceError::Descriptor(
            "invalid virtio-mem request descriptor",
        ));
    }
    if response.flags & WRITE_FLAG == 0 || response.length != RESPONSE_SIZE as u32 {
        return Err(DeviceError::Descriptor(
            "invalid virtio-mem response descriptor",
        ));
    }
    let lease = memory.lease(DmaRange::new(request.address, REQUEST_SIZE))?;
    let mut bytes = [0; REQUEST_SIZE];
    let mut offset = 0;
    for part in lease.parts() {
        let source = unsafe { part.read_slice() };
        bytes[offset..offset + source.len()].copy_from_slice(source);
        offset += source.len();
    }
    let ty = u16::from_le_bytes(bytes[0..2].try_into().expect("request type"));
    let addr = u64::from_le_bytes(bytes[8..16].try_into().expect("request address"));
    let blocks = u16::from_le_bytes(bytes[16..18].try_into().expect("request blocks"));
    match ty {
        VIRTIO_MEM_REQ_PLUG => Ok(MemoryRequest::Plug { addr, blocks }),
        VIRTIO_MEM_REQ_UNPLUG => Ok(MemoryRequest::Unplug { addr, blocks }),
        VIRTIO_MEM_REQ_UNPLUG_ALL => Ok(MemoryRequest::UnplugAll),
        VIRTIO_MEM_REQ_STATE => Ok(MemoryRequest::State { addr, blocks }),
        _ => Err(DeviceError::Descriptor("unknown virtio-mem request")),
    }
}

fn write_response(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    response: MemoryResponse,
) -> Result<(), DeviceError> {
    let descriptor = chain.descriptors[1];
    let mut bytes = [0; RESPONSE_SIZE];
    let response_type = match response {
        MemoryResponse::Ack => VIRTIO_MEM_RESP_ACK,
        MemoryResponse::Nack => VIRTIO_MEM_RESP_NACK,
        MemoryResponse::Busy => VIRTIO_MEM_RESP_BUSY,
        MemoryResponse::Error => VIRTIO_MEM_RESP_ERROR,
        MemoryResponse::State(_) => VIRTIO_MEM_RESP_ACK,
    };
    bytes[0..2].copy_from_slice(&response_type.to_le_bytes());
    if let MemoryResponse::State(state) = response {
        bytes[8..10].copy_from_slice(&state.to_le_bytes());
    }
    let mut lease = memory.lease(DmaRange::new(descriptor.address, RESPONSE_SIZE))?;
    let mut offset = 0;
    for part in lease.parts_mut() {
        let target = unsafe { part.write_slice() };
        target.copy_from_slice(&bytes[offset..offset + target.len()]);
        offset += target.len();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_the_standard_56_byte_shape() {
        let config = MemoryConfig {
            block_size: 2 << 20,
            node_id: 0,
            addr: 1 << 30,
            region_size: 1 << 30,
            usable_region_size: 1 << 30,
            plugged_size: 0,
            requested_size: 0,
        };
        let state = MemoryConfigState::new(config).expect("config");
        let view = ConfigView { state };
        let mut bytes = [0; CONFIG_SIZE];
        view.read(0, &mut bytes).expect("read config");
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().expect("block size")),
            2 << 20
        );
    }

    #[test]
    fn config_update_advances_generation() {
        let initial = MemoryConfig {
            block_size: 4096,
            node_id: 0,
            addr: 0,
            region_size: 4096,
            usable_region_size: 4096,
            plugged_size: 0,
            requested_size: 0,
        };
        let state = MemoryConfigState::new(initial).expect("config");
        let mut updates = state.subscribe();
        state
            .update(MemoryConfig {
                requested_size: 4096,
                ..initial
            })
            .expect("update");
        assert_eq!(*updates.borrow_and_update(), 1);
    }
}
