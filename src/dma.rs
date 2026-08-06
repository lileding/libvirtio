use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::Notify;

use crate::error::{DeviceDownReason, DeviceError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DmaRange {
    pub gpa: u64,
    pub length: usize,
}

impl DmaRange {
    pub const fn new(gpa: u64, length: usize) -> Self {
        Self { gpa, length }
    }

    pub fn end(self) -> Option<u64> {
        self.gpa.checked_add(u64::try_from(self.length).ok()?)
    }
}

/// One directly mapped GPA interval supplied by an embedding transport.
#[derive(Clone, Copy, Debug)]
pub struct DmaSegment {
    imm_gpa: u64,
    raw_imm_base: NonNull<u8>,
    imm_length: usize,
}

unsafe impl Send for DmaSegment {}
unsafe impl Sync for DmaSegment {}

impl DmaSegment {
    /// # Safety
    ///
    /// `base..base + length` must map exactly the supplied guest GPA range for
    /// the generation which owns this segment.
    pub unsafe fn new(gpa: u64, base: NonNull<u8>, length: usize) -> Self {
        Self {
            imm_gpa: gpa,
            raw_imm_base: base,
            imm_length: length,
        }
    }

    pub const fn gpa(&self) -> u64 {
        self.imm_gpa
    }

    pub const fn length(&self) -> usize {
        self.imm_length
    }

    fn end(&self) -> Option<u64> {
        self.imm_gpa
            .checked_add(u64::try_from(self.imm_length).ok()?)
    }
}

struct DmaLeaseState {
    atomic_mut_revoked: AtomicBool,
    atomic_mut_active: AtomicUsize,
    own_imm_drained: Notify,
}

/// A generation-scoped collection of direct guest-memory mappings.
///
/// The embedding transport calls `revoke()` before removing mappings, then
/// waits for `wait_for_drain()` before unmapping them.  Device code obtains a
/// `DmaLease` for every request and must drop that lease before its task yields
/// control to teardown.
pub struct DmaMemory {
    imm_generation: u64,
    own_imm_segments: Arc<[DmaSegment]>,
    arc_imm_lease_state: Arc<DmaLeaseState>,
}

impl DmaMemory {
    pub fn new(generation: u64, mut segments: Vec<DmaSegment>) -> Result<Self, DeviceError> {
        if segments.is_empty() {
            return Err(DeviceError::InvalidLayout("DMA mapping has no segments"));
        }
        segments.sort_unstable_by_key(DmaSegment::gpa);
        for (index, segment) in segments.iter().enumerate() {
            if segment.length() == 0 || segment.end().is_none() {
                return Err(DeviceError::InvalidLayout("invalid DMA segment"));
            }
            if index != 0 && segments[index - 1].end().expect("checked above") > segment.gpa() {
                return Err(DeviceError::InvalidLayout("overlapping DMA segments"));
            }
        }
        Ok(Self {
            imm_generation: generation,
            own_imm_segments: segments.into(),
            arc_imm_lease_state: Arc::new(DmaLeaseState {
                atomic_mut_revoked: AtomicBool::new(false),
                atomic_mut_active: AtomicUsize::new(0),
                own_imm_drained: Notify::new(),
            }),
        })
    }

    pub const fn generation(&self) -> u64 {
        self.imm_generation
    }

    pub fn segments(&self) -> &[DmaSegment] {
        &self.own_imm_segments
    }

    pub fn is_revoked(&self) -> bool {
        self.arc_imm_lease_state
            .atomic_mut_revoked
            .load(Ordering::Acquire)
    }

    /// Prevent future leases.  The transport must call `wait_for_drain()`
    /// before it unmaps any segment.
    pub fn revoke(&self) {
        self.arc_imm_lease_state
            .atomic_mut_revoked
            .store(true, Ordering::Release);
    }

    pub async fn wait_for_drain(&self) {
        loop {
            let notified = self.arc_imm_lease_state.own_imm_drained.notified();
            if self
                .arc_imm_lease_state
                .atomic_mut_active
                .load(Ordering::Acquire)
                == 0
            {
                return;
            }
            notified.await;
        }
    }

    pub fn check_range(&self, range: DmaRange) -> Result<(), DeviceError> {
        self.translate(range).map(|_| ())
    }

    pub fn lease(&self, range: DmaRange) -> Result<DmaLease, DeviceError> {
        let state = &self.arc_imm_lease_state;
        if state.atomic_mut_revoked.load(Ordering::Acquire) {
            return Err(DeviceError::Down(DeviceDownReason::Revoked));
        }
        state.atomic_mut_active.fetch_add(1, Ordering::AcqRel);
        if state.atomic_mut_revoked.load(Ordering::Acquire) {
            DmaLease::drop_active(state);
            return Err(DeviceError::Down(DeviceDownReason::Revoked));
        }
        match self.translate(range) {
            Ok(parts) => Ok(DmaLease {
                own_imm_parts: parts,
                arc_imm_lease_state: Arc::clone(state),
            }),
            Err(error) => {
                DmaLease::drop_active(state);
                Err(error)
            }
        }
    }

    fn translate(&self, range: DmaRange) -> Result<Vec<DmaPart>, DeviceError> {
        let end = range.end().ok_or(DeviceError::DmaRange {
            gpa: range.gpa,
            length: range.length,
        })?;
        let mut cursor = range.gpa;
        let mut parts = Vec::new();
        while cursor < end {
            let segment = self
                .own_imm_segments
                .iter()
                .find(|segment| {
                    segment.gpa() <= cursor && segment.end().is_some_and(|end| cursor < end)
                })
                .ok_or(DeviceError::DmaRange {
                    gpa: range.gpa,
                    length: range.length,
                })?;
            let segment_end = segment.end().expect("validated DMA segment");
            let part_end = segment_end.min(end);
            let offset =
                usize::try_from(cursor - segment.gpa()).map_err(|_| DeviceError::DmaRange {
                    gpa: range.gpa,
                    length: range.length,
                })?;
            let length = usize::try_from(part_end - cursor).map_err(|_| DeviceError::DmaRange {
                gpa: range.gpa,
                length: range.length,
            })?;
            parts.push(DmaPart {
                imm_gpa: cursor,
                raw_imm_base: unsafe {
                    NonNull::new_unchecked(segment.raw_imm_base.as_ptr().add(offset))
                },
                imm_length: length,
            });
            cursor = part_end;
        }
        Ok(parts)
    }
}

/// An active direct-DMA request.  Its destructor releases the generation hold.
pub struct DmaLease {
    own_imm_parts: Vec<DmaPart>,
    arc_imm_lease_state: Arc<DmaLeaseState>,
}

impl DmaLease {
    pub fn parts(&self) -> &[DmaPart] {
        &self.own_imm_parts
    }

    pub fn parts_mut(&mut self) -> &mut [DmaPart] {
        &mut self.own_imm_parts
    }

    fn drop_active(state: &DmaLeaseState) {
        if state.atomic_mut_active.fetch_sub(1, Ordering::AcqRel) == 1 {
            state.own_imm_drained.notify_waiters();
        }
    }
}

impl Drop for DmaLease {
    fn drop(&mut self) {
        Self::drop_active(&self.arc_imm_lease_state);
    }
}

/// A contiguous part of a DMA lease.  It may be converted to an I/O vector
/// only while its parent lease is alive and before the task awaits teardown.
pub struct DmaPart {
    imm_gpa: u64,
    raw_imm_base: NonNull<u8>,
    imm_length: usize,
}

impl DmaPart {
    pub const fn gpa(&self) -> u64 {
        self.imm_gpa
    }

    pub const fn length(&self) -> usize {
        self.imm_length
    }

    /// # Safety
    ///
    /// The caller must not retain the returned slice after its parent lease is
    /// dropped or across a teardown boundary.
    pub unsafe fn read_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.raw_imm_base.as_ptr(), self.imm_length) }
    }

    /// # Safety
    ///
    /// The caller must have exclusive access to this range and must not retain
    /// the returned slice after its parent lease is dropped or across teardown.
    pub unsafe fn write_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.raw_imm_base.as_ptr(), self.imm_length) }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::{DmaMemory, DmaRange, DmaSegment};
    use crate::error::{DeviceDownReason, DeviceError};

    fn memory(first: &mut [u8], second: &mut [u8]) -> DmaMemory {
        DmaMemory::new(
            7,
            vec![
                unsafe { DmaSegment::new(0x1000, NonNull::from(&mut first[0]), first.len()) },
                unsafe { DmaSegment::new(0x1010, NonNull::from(&mut second[0]), second.len()) },
            ],
        )
        .expect("valid DMA memory")
    }

    #[tokio::test]
    async fn translates_across_segments_and_drains_before_unmap() {
        let mut first = [0u8; 16];
        let mut second = [0u8; 16];
        first[8] = b'a';
        second[0] = b'b';
        let dma = memory(&mut first, &mut second);

        let lease = dma.lease(DmaRange::new(0x1008, 16)).expect("lease");
        assert_eq!(lease.parts().len(), 2);
        assert_eq!(lease.parts()[0].length(), 8);
        assert_eq!(lease.parts()[1].length(), 8);
        assert_eq!(unsafe { lease.parts()[0].read_slice() }[0], b'a');
        assert_eq!(unsafe { lease.parts()[1].read_slice() }[0], b'b');

        dma.revoke();
        assert!(matches!(
            dma.lease(DmaRange::new(0x1000, 1)),
            Err(DeviceError::Down(DeviceDownReason::Revoked))
        ));
        drop(lease);
        dma.wait_for_drain().await;
    }
}
