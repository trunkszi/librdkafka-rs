//! Anonymous, immutable backing for curl's file-based client identity interface.

use curl::easy::Easy;

use crate::{Error, Result, tls::TlsSettings};

/// Kept after the event loop in its owner's drop order so every transfer and
/// cached connection releases its curl state before identity descriptors close.
pub(super) struct Identity {
    #[cfg(target_os = "linux")]
    files: Option<(memfd::Memfd, memfd::Memfd)>,
}

impl Identity {
    pub(super) fn new(tls: &TlsSettings) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let files = if tls.client_cert_pem.is_empty() {
                None
            } else {
                Some((
                    sealed_file("rdkafka-client-cert", &tls.client_cert_pem)?,
                    sealed_file("rdkafka-client-key", &tls.client_key_pem)?,
                ))
            };
            Ok(Self { files })
        }
        #[cfg(not(target_os = "linux"))]
        {
            if !tls.client_cert_pem.is_empty() {
                return Err(Error::Tls(
                    "HTTP mutual TLS requires Linux anonymous file support".into(),
                ));
            }
            Ok(Self {})
        }
    }

    pub(super) fn configure(&self, easy: &mut Easy) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            if let Some((cert, key)) = &self.files {
                easy.ssl_cert_type("PEM").map_err(super::curl_error)?;
                easy.ssl_key_type("PEM").map_err(super::curl_error)?;
                easy.ssl_cert(format!("/proc/self/fd/{}", cert.as_raw_fd()))
                    .map_err(super::curl_error)?;
                easy.ssl_key(format!("/proc/self/fd/{}", key.as_raw_fd()))
                    .map_err(super::curl_error)?;
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = easy;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn sealed_file(name: &str, pem: &[u8]) -> Result<memfd::Memfd> {
    use std::io::Write;

    use memfd::{FileSeal, MemfdOptions};

    let file = MemfdOptions::new()
        .allow_sealing(true)
        .close_on_exec(true)
        .create(name)
        .map_err(|_| Error::Tls("could not create anonymous client identity file".into()))?;
    file.as_file()
        .write_all(pem)
        .map_err(|_| Error::Tls("could not write anonymous client identity file".into()))?;
    file.add_seals(&[
        FileSeal::SealWrite,
        FileSeal::SealGrow,
        FileSeal::SealShrink,
        FileSeal::SealSeal,
    ])
    .map_err(|_| Error::Tls("could not seal anonymous client identity file".into()))?;
    Ok(file)
}
