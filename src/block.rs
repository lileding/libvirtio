use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
use crate::error::{DeviceDownReason, DeviceError};

pub const VIRTIO_BLK_F_FLUSH: u64 = 1 << 9;

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
    own_mut_file: Mutex<File>,
    own_imm_resources: DeviceResources,
    imm_read_only: bool,
    atomic_mut_kicked: AtomicBool,
    atomic_mut_down: AtomicBool,
}

impl BlockDevice {
    async fn open(
        declaration: &BlockDeclaration,
        resources: DeviceResources,
    ) -> Result<Self, DeviceError> {
        let mut options = OpenOptions::new();
        options.read(true);
        if !declaration.imm_read_only {
            options.write(true);
        }
        let file = options.open(&declaration.own_imm_path).await?;
        Ok(Self {
            own_mut_file: Mutex::new(file),
            own_imm_resources: resources,
            imm_read_only: declaration.imm_read_only,
            atomic_mut_kicked: AtomicBool::new(false),
            atomic_mut_down: AtomicBool::new(false),
        })
    }

    pub fn take_kick(&self) -> bool {
        self.atomic_mut_kicked.swap(false, Ordering::AcqRel)
    }

    pub fn resources(&self) -> &DeviceResources {
        &self.own_imm_resources
    }

    pub async fn read_at(&self, offset: u64, buffer: &mut [u8]) -> Result<(), DeviceError> {
        self.check_live()?;
        let mut file = self.own_mut_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read_exact(buffer).await?;
        Ok(())
    }

    pub async fn write_at(&self, offset: u64, buffer: &[u8]) -> Result<(), DeviceError> {
        self.check_live()?;
        if self.imm_read_only {
            return Err(DeviceError::Io(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            )));
        }
        let mut file = self.own_mut_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(buffer).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), DeviceError> {
        self.check_live()?;
        let file = self.own_mut_file.lock().await;
        file.sync_data().await?;
        Ok(())
    }

    fn check_live(&self) -> Result<(), DeviceError> {
        if self.atomic_mut_down.load(Ordering::Acquire) {
            return Err(DeviceError::Down(DeviceDownReason::Revoked));
        }
        Ok(())
    }
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
        Ok(Box::new(BlockDevice::open(self, resources).await?))
    }
}

#[async_trait]
impl DeviceInstance for BlockDevice {
    fn kick(&self) {
        self.atomic_mut_kicked.store(true, Ordering::Release);
    }

    async fn shutdown(&self, _reason: DeviceDownReason) -> Result<(), DeviceError> {
        self.atomic_mut_down.store(true, Ordering::Release);
        Ok(())
    }
}

pub type SharedBlockDevice = Arc<BlockDevice>;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{BlockDeclaration, BlockDevice};
    use crate::device::{DeviceInstance, DeviceResources};
    use crate::dma::{DmaMemory, DmaRange};
    use crate::error::{DeviceDownReason, DeviceError};
    use crate::interrupt::{Interrupt, InterruptNotifier};
    use crate::queue::QueueLayout;

    struct TestNotifier;

    #[async_trait::async_trait]
    impl InterruptNotifier for TestNotifier {
        async fn notify(&self, _interrupt: Interrupt) -> Result<(), DeviceError> {
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

    fn resources(memory: &mut [u8]) -> DeviceResources {
        DeviceResources {
            queues: vec![QueueLayout {
                index: 0,
                size: 8,
                descriptors: DmaRange::new(0, 8 * 16),
                available: DmaRange::new(128, 8),
                used: DmaRange::new(136, 8),
            }],
            dma: unsafe { DmaMemory::new(NonNull::from(&mut memory[0]), memory.len(), 1) },
            interrupts: vec![Arc::new(TestNotifier)],
            negotiated_features: super::VIRTIO_BLK_F_FLUSH,
        }
    }

    #[tokio::test]
    async fn reads_writes_flushes_and_stops() {
        let path = image_path();
        fs::write(&path, vec![0u8; 4096]).expect("create image");
        let declaration = BlockDeclaration::new(&path, 1, 128);
        let mut memory = [0u8; 256];
        let device = BlockDevice::open(&declaration, resources(&mut memory))
            .await
            .expect("open image");
        assert_eq!(device.resources().queues.len(), 1);

        device.write_at(512, b"vmm").await.expect("write");
        device.flush().await.expect("flush");
        let mut buffer = [0u8; 3];
        device.read_at(512, &mut buffer).await.expect("read");
        assert_eq!(&buffer, b"vmm");

        device.kick();
        assert!(device.take_kick());
        assert!(!device.take_kick());
        device
            .shutdown(DeviceDownReason::Revoked)
            .await
            .expect("shutdown");
        assert!(matches!(
            device.flush().await,
            Err(DeviceError::Down(DeviceDownReason::Revoked))
        ));
        fs::remove_file(path).expect("remove image");
    }
}
