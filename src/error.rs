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

    #[error("DMA GPA range {gpa:#x} length={length} is not mapped")]
    DmaRange { gpa: u64, length: usize },

    #[error("DMA mapping is not aligned to {alignment} bytes")]
    DmaAlignment { alignment: usize },

    #[error("descriptor chain is malformed: {0}")]
    Descriptor(&'static str),

    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),

    #[error("blocking device worker failed: {0}")]
    Worker(String),
}
