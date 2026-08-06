use std::ptr::NonNull;

use crate::error::DeviceError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DmaRange {
    pub offset: usize,
    pub length: usize,
}

impl DmaRange {
    pub const fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    pub fn end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

/// A generation-scoped direct view of guest memory.
///
/// The embedding transport guarantees the mapping remains valid for the
/// instance generation.  Callers must never retain slices obtained here across
/// an await point, or after the transport has revoked the generation.
#[derive(Debug)]
pub struct DmaMemory {
    raw_imm_base: NonNull<u8>,
    imm_length: usize,
    imm_generation: u64,
}

unsafe impl Send for DmaMemory {}
unsafe impl Sync for DmaMemory {}

impl DmaMemory {
    /// # Safety
    ///
    /// `base..base + length` must be a valid DMA mapping for the entire device
    /// generation.  The caller owns revocation and must stop all device tasks
    /// before unmapping this memory.
    pub unsafe fn new(base: NonNull<u8>, length: usize, generation: u64) -> Self {
        Self {
            raw_imm_base: base,
            imm_length: length,
            imm_generation: generation,
        }
    }

    pub const fn length(&self) -> usize {
        self.imm_length
    }

    pub const fn generation(&self) -> u64 {
        self.imm_generation
    }

    pub fn check_range(&self, range: DmaRange) -> Result<(), DeviceError> {
        match range.end() {
            Some(end) if end <= self.imm_length => Ok(()),
            _ => Err(DeviceError::DmaRange {
                offset: range.offset,
                length: range.length,
                memory_length: self.imm_length,
            }),
        }
    }

    /// # Safety
    ///
    /// The caller must not retain the returned slice across an await point or
    /// after the transport revokes this generation.
    pub unsafe fn read_slice(&self, range: DmaRange) -> Result<&[u8], DeviceError> {
        self.check_range(range)?;
        Ok(unsafe {
            std::slice::from_raw_parts(self.raw_imm_base.as_ptr().add(range.offset), range.length)
        })
    }

    /// # Safety
    ///
    /// The caller must have exclusive access to the range and must not retain
    /// the returned slice across an await point or after revocation.
    pub unsafe fn write_slice(&mut self, range: DmaRange) -> Result<&mut [u8], DeviceError> {
        self.check_range(range)?;
        Ok(unsafe {
            std::slice::from_raw_parts_mut(
                self.raw_imm_base.as_ptr().add(range.offset),
                range.length,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::{DmaMemory, DmaRange};

    #[test]
    fn rejects_out_of_bounds_range() {
        let mut memory = [0u8; 32];
        let dma = unsafe { DmaMemory::new(NonNull::from(&mut memory[0]), memory.len(), 7) };

        assert!(dma.check_range(DmaRange::new(16, 16)).is_ok());
        assert!(dma.check_range(DmaRange::new(17, 16)).is_err());
        assert!(dma.check_range(DmaRange::new(usize::MAX, 1)).is_err());
        assert_eq!(dma.generation(), 7);
    }
}
