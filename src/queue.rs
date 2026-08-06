use crate::dma::{DmaMemory, DmaRange};
use crate::error::DeviceError;

const VIRTQ_DESC_SIZE: usize = 16;
const VIRTQ_DESC_F_NEXT: u16 = 1;

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
        if self.descriptors.length < usize::from(self.size) * VIRTQ_DESC_SIZE {
            return Err(DeviceError::InvalidQueue {
                queue: self.index,
                reason: "descriptor table is too small",
            });
        }
        memory.check_range(self.descriptors)?;
        memory.check_range(self.available)?;
        memory.check_range(self.used)?;
        Ok(())
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
                return Ok(DescriptorChain { descriptors });
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
        let lease = memory.lease(DmaRange::new(gpa, VIRTQ_DESC_SIZE))?;
        let bytes = unsafe { lease.parts()[0].read_slice() };
        Ok(Descriptor {
            address: u64::from_le_bytes(bytes[0..8].try_into().expect("descriptor address")),
            length: u32::from_le_bytes(bytes[8..12].try_into().expect("descriptor length")),
            flags: u16::from_le_bytes(bytes[12..14].try_into().expect("descriptor flags")),
            next: u16::from_le_bytes(bytes[14..16].try_into().expect("descriptor next")),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::{QueueLayout, VirtQueue};
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
            available: DmaRange::new(0x1080, 8),
            used: DmaRange::new(0x1088, 8),
        };
        let queue = VirtQueue::new(layout, &dma).expect("valid queue");
        (dma, queue)
    }

    #[test]
    fn reads_a_two_descriptor_chain() {
        let mut memory = [0u8; 256];
        memory[0..8].copy_from_slice(&0x1000u64.to_le_bytes());
        memory[8..12].copy_from_slice(&512u32.to_le_bytes());
        memory[12..14].copy_from_slice(&1u16.to_le_bytes());
        memory[14..16].copy_from_slice(&1u16.to_le_bytes());
        memory[16..24].copy_from_slice(&0x2000u64.to_le_bytes());
        memory[24..28].copy_from_slice(&64u32.to_le_bytes());
        let (dma, queue) = queue(&mut memory);

        let chain = unsafe { queue.read_chain(&dma, 0) }.expect("valid chain");
        assert_eq!(chain.descriptors.len(), 2);
        assert_eq!(chain.descriptors[1].address, 0x2000);
    }
}
