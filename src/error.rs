use std::io;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceDownReason {
    Stop,
    Reset,
    Revoked,
    SurpriseRemoval,
}

#[derive(Debug, Error)]
pub enum DeviceError {
    #[error("device is down: {0:?}")]
    Down(DeviceDownReason),

    #[error("invalid device layout: {0}")]
    InvalidLayout(&'static str),

    #[error("invalid queue {queue}: {reason}")]
    InvalidQueue { queue: u16, reason: &'static str },

    #[error("DMA range offset={offset} length={length} exceeds {memory_length}")]
    DmaRange {
        offset: usize,
        length: usize,
        memory_length: usize,
    },

    #[error("DMA mapping is not aligned to {alignment} bytes")]
    DmaAlignment { alignment: usize },

    #[error("descriptor chain is malformed: {0}")]
    Descriptor(&'static str),

    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
}
