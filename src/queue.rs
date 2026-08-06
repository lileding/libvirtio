use std::sync::atomic::{Ordering, fence};

use crate::dma::{DmaLease, DmaMemory, DmaRange};
use crate::error::DeviceError;

const VIRTQ_DESC_SIZE: usize = 16;
const VIRTQ_DESC_F_NEXT: u16 = 1;
const VIRTQ_AVAIL_HEADER_SIZE: usize = 4;
const VIRTQ_USED_HEADER_SIZE: usize = 4;
const VIRTQ_USED_ELEMENT_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueLayout {
    pub index: u16,
    pub size: u16,
    pub descriptors: DmaRange,
    pub available: DmaRange,
    pub used: DmaRange,
}

impl QueueLayout {
    pub fn validate(self, memory: &DmaMemory) -> Result<(), DeviceError> {
        if self.size == 0 || !self.size.is_power_of_two() {
            return Err(DeviceError::InvalidQueue {
                queue: self.index,
                reason: "size must be a non-zero power of two",
            });
        }
        if self.descriptors.length < usize::from(self.size) * VIRTQ_DESC_SIZE
            || self.available.length < VIRTQ_AVAIL_HEADER_SIZE + usize::from(self.size) * 2
            || self.used.length
                < VIRTQ_USED_HEADER_SIZE + usize::from(self.size) * VIRTQ_USED_ELEMENT_SIZE
        {
            return Err(DeviceError::InvalidQueue {
                queue: self.index,
                reason: "virtqueue ring is too small",
            });
        }
        memory.check_range(self.descriptors)?;
        memory.check_range(self.available)?;
        memory.check_range(self.used)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueState {
    mut_last_available: u16,
    mut_next_used: u16,
    mut_inflight: bool,
}

impl QueueState {
    pub const fn new() -> Self {
        Self {
            mut_last_available: 0,
            mut_next_used: 0,
            mut_inflight: false,
        }
    }

    pub const fn inflight(self) -> bool {
        self.mut_inflight
    }
}

impl Default for QueueState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Descriptor {
    pub address: u64,
    pub length: u32,
    pub flags: u16,
    pub next: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorChain {
    pub head: u16,
    pub descriptors: Vec<Descriptor>,
}

#[derive(Clone, Copy)]
pub struct VirtQueue {
    imm_layout: QueueLayout,
}

impl VirtQueue {
    pub fn new(layout: QueueLayout, memory: &DmaMemory) -> Result<Self, DeviceError> {
        layout.validate(memory)?;
        Ok(Self { imm_layout: layout })
    }

    pub const fn layout(self) -> QueueLayout {
        self.imm_layout
    }

    pub fn pop(
        self,
        memory: &DmaMemory,
        state: &mut QueueState,
    ) -> Result<Option<DescriptorChain>, DeviceError> {
        if state.mut_inflight {
            return Ok(None);
        }
        let available_index = read_u16(memory, self.imm_layout.available.gpa + 2)?;
        if available_index == state.mut_last_available {
            return Ok(None);
        }
        if available_index.wrapping_sub(state.mut_last_available) > self.imm_layout.size {
            return Err(DeviceError::InvalidQueue {
                queue: self.imm_layout.index,
                reason: "available index exceeds queue size",
            });
        }
        let slot = usize::from(state.mut_last_available % self.imm_layout.size);
        let head = read_u16(
            memory,
            self.imm_layout.available.gpa + VIRTQ_AVAIL_HEADER_SIZE as u64 + (slot * 2) as u64,
        )?;
        let chain = unsafe { self.read_chain(memory, head)? };
        state.mut_last_available = state.mut_last_available.wrapping_add(1);
        state.mut_inflight = true;
        Ok(Some(chain))
    }

    pub fn complete(
        self,
        memory: &DmaMemory,
        state: &mut QueueState,
        chain: &DescriptorChain,
        used_length: u32,
    ) -> Result<(), DeviceError> {
        if !state.mut_inflight {
            return Err(DeviceError::InvalidQueue {
                queue: self.imm_layout.index,
                reason: "completion without an inflight request",
            });
        }
        let slot = usize::from(state.mut_next_used % self.imm_layout.size);
        let offset = self.imm_layout.used.gpa
            + VIRTQ_USED_HEADER_SIZE as u64
            + (slot * VIRTQ_USED_ELEMENT_SIZE) as u64;
        write_bytes(memory, offset, &u32::from(chain.head).to_le_bytes())?;
        write_bytes(memory, offset + 4, &used_length.to_le_bytes())?;
        fence(Ordering::Release);
        state.mut_next_used = state.mut_next_used.wrapping_add(1);
        write_bytes(
            memory,
            self.imm_layout.used.gpa + 2,
            &state.mut_next_used.to_le_bytes(),
        )?;
        state.mut_inflight = false;
        Ok(())
    }

    /// # Safety
    ///
    /// The transport must ensure descriptor memory remains mapped for this
    /// generation and that the caller does not retain raw DMA views across an
    /// await point.
    pub unsafe fn read_chain(
        self,
        memory: &DmaMemory,
        head: u16,
    ) -> Result<DescriptorChain, DeviceError> {
        if head >= self.imm_layout.size {
            return Err(DeviceError::Descriptor("head index exceeds queue size"));
        }

        let mut current = head;
        let mut descriptors = Vec::new();
        for _ in 0..self.imm_layout.size {
            let descriptor = unsafe { self.read_descriptor(memory, current)? };
            let has_next = descriptor.flags & VIRTQ_DESC_F_NEXT != 0;
            descriptors.push(descriptor);
            if !has_next {
                return Ok(DescriptorChain { head, descriptors });
            }
            if descriptor.next >= self.imm_layout.size {
                return Err(DeviceError::Descriptor("next index exceeds queue size"));
            }
            current = descriptor.next;
        }
        Err(DeviceError::Descriptor("descriptor chain loop"))
    }

    unsafe fn read_descriptor(
        self,
        memory: &DmaMemory,
        index: u16,
    ) -> Result<Descriptor, DeviceError> {
        let gpa = self
            .imm_layout
            .descriptors
            .gpa
            .checked_add(
                u64::try_from(usize::from(index) * VIRTQ_DESC_SIZE)
                    .expect("descriptor offset fits u64"),
            )
            .ok_or(DeviceError::Descriptor("descriptor GPA overflow"))?;
        let bytes = read_bytes(memory, gpa, VIRTQ_DESC_SIZE)?;
        Ok(Descriptor {
            address: u64::from_le_bytes(bytes[0..8].try_into().expect("descriptor address")),
            length: u32::from_le_bytes(bytes[8..12].try_into().expect("descriptor length")),
            flags: u16::from_le_bytes(bytes[12..14].try_into().expect("descriptor flags")),
            next: u16::from_le_bytes(bytes[14..16].try_into().expect("descriptor next")),
        })
    }
}

fn read_u16(memory: &DmaMemory, gpa: u64) -> Result<u16, DeviceError> {
    Ok(u16::from_le_bytes(
        read_bytes(memory, gpa, 2)?.try_into().expect("u16"),
    ))
}

fn read_bytes(memory: &DmaMemory, gpa: u64, length: usize) -> Result<Vec<u8>, DeviceError> {
    let lease = memory.lease(DmaRange::new(gpa, length))?;
    copy_from_lease(&lease, length)
}

fn write_bytes(memory: &DmaMemory, gpa: u64, bytes: &[u8]) -> Result<(), DeviceError> {
    let mut lease = memory.lease(DmaRange::new(gpa, bytes.len()))?;
    let mut offset = 0usize;
    for part in lease.parts_mut() {
        let target = unsafe { part.write_slice() };
        let end = offset + target.len();
        target.copy_from_slice(&bytes[offset..end]);
        offset = end;
    }
    if offset != bytes.len() {
        return Err(DeviceError::Descriptor("short virtqueue write"));
    }
    Ok(())
}

fn copy_from_lease(lease: &DmaLease, length: usize) -> Result<Vec<u8>, DeviceError> {
    let mut result = Vec::with_capacity(length);
    for part in lease.parts() {
        result.extend_from_slice(unsafe { part.read_slice() });
    }
    if result.len() != length {
        return Err(DeviceError::Descriptor("short virtqueue read"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::{QueueLayout, QueueState, VirtQueue};
    use crate::dma::{DmaMemory, DmaRange, DmaSegment};

    fn queue(memory: &mut [u8]) -> (DmaMemory, VirtQueue) {
        let dma = DmaMemory::new(
            1,
            vec![unsafe { DmaSegment::new(0x1000, NonNull::from(&mut memory[0]), memory.len()) }],
        )
        .expect("DMA memory");
        let layout = QueueLayout {
            index: 0,
            size: 8,
            descriptors: DmaRange::new(0x1000, 8 * 16),
            available: DmaRange::new(0x1080, 4 + 8 * 2),
            used: DmaRange::new(0x10a0, 4 + 8 * 8),
        };
        let queue = VirtQueue::new(layout, &dma).expect("valid queue");
        (dma, queue)
    }

    #[test]
    fn pops_and_completes_a_chain() {
        let mut memory = [0u8; 512];
        memory[0..8].copy_from_slice(&0x1100u64.to_le_bytes());
        memory[8..12].copy_from_slice(&512u32.to_le_bytes());
        memory[12..14].copy_from_slice(&0u16.to_le_bytes());
        memory[0x82..0x84].copy_from_slice(&1u16.to_le_bytes());
        memory[0x84..0x86].copy_from_slice(&0u16.to_le_bytes());
        let (dma, queue) = queue(&mut memory);
        let mut state = QueueState::new();

        let chain = queue.pop(&dma, &mut state).expect("pop").expect("chain");
        assert_eq!(chain.head, 0);
        assert!(state.inflight());
        queue
            .complete(&dma, &mut state, &chain, 513)
            .expect("complete");
        assert!(!state.inflight());
        let used = dma.lease(DmaRange::new(0x10a0, 12)).expect("used ring");
        let bytes = unsafe { used.parts()[0].read_slice() };
        assert_eq!(u16::from_le_bytes(bytes[2..4].try_into().expect("idx")), 1);
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().expect("head")), 0);
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().expect("length")),
            513
        );
    }
}
