use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

pub(super) const BUFFER_SIZE: usize = 64 * 1024;

fn would_block(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn wait(descriptors: &mut [libc::pollfd], timeout: i32) -> io::Result<()> {
    wait_with(descriptors, timeout, |descriptors, timeout| {
        // SAFETY: poll receives the valid mutable slice and its exact length.
        let result =
            unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, timeout) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result)
        }
    })
}

fn wait_with(
    descriptors: &mut [libc::pollfd],
    timeout: i32,
    poll: impl FnOnce(&mut [libc::pollfd], i32) -> io::Result<i32>,
) -> io::Result<()> {
    match poll(descriptors, timeout) {
        Ok(0) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "broker TLS handshake timed out",
        )),
        Ok(_) => {
            if descriptors
                .iter()
                .any(|fd| fd.revents & libc::POLLNVAL != 0)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "closed TLS descriptor",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::Interrupted => {
            // The caller must recheck its deadline and cancellation state.
            // poll does not promise valid revents when interrupted.
            for descriptor in descriptors {
                descriptor.revents = 0;
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn handshake(
    connection: &mut rustls::ClientConnection,
    mut remote: &TcpStream,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "broker TLS timeout exceeds clock range",
        )
    })?;
    while connection.is_handshaking() || connection.wants_write() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "broker TLS handshake timed out",
            ));
        }
        let mut descriptors = [libc::pollfd {
            fd: remote.as_raw_fd(),
            events: libc::POLLIN
                | if connection.wants_write() {
                    libc::POLLOUT
                } else {
                    0
                },
            revents: 0,
        }];
        wait(
            &mut descriptors,
            remaining.as_millis().clamp(1, i32::MAX as u128) as i32,
        )?;
        if descriptors[0].revents & libc::POLLOUT != 0 && connection.wants_write() {
            match connection.write_tls(&mut remote) {
                Ok(_) => {}
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if descriptors[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            match connection.read_tls(&mut remote) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "broker closed TLS handshake",
                    ));
                }
                Ok(_) => {
                    connection.process_new_packets().map_err(tls_error)?;
                }
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

struct Pending {
    bytes: Box<[u8; BUFFER_SIZE]>,
    start: usize,
    end: usize,
}

impl Pending {
    fn new() -> Self {
        Self {
            bytes: Box::new([0; BUFFER_SIZE]),
            start: 0,
            end: 0,
        }
    }
    fn empty(&self) -> bool {
        self.start == self.end
    }
    fn drain(&mut self, writer: &mut impl Write) -> io::Result<()> {
        if !self.empty() {
            match writer.write(&self.bytes[self.start..self.end]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => self.start += count,
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    fn fill(&mut self, reader: &mut impl Read) -> io::Result<bool> {
        if !self.empty() {
            return Ok(true);
        }
        self.start = 0;
        self.end = 0;
        match reader.read(&mut *self.bytes) {
            Ok(0) => Ok(false),
            Ok(count) => {
                self.end = count;
                Ok(true)
            }
            Err(error) if would_block(&error) => {
                self.end = 0;
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }
}

pub(super) fn run(
    connection: rustls::ClientConnection,
    remote: TcpStream,
    local: &UnixStream,
    stopping: &AtomicBool,
) -> io::Result<()> {
    run_loop(connection, remote, local, stopping, |_| {})
}

fn run_loop(
    mut connection: rustls::ClientConnection,
    mut remote: TcpStream,
    mut local: &UnixStream,
    stopping: &AtomicBool,
    mut before_poll: impl FnMut(&[libc::pollfd]),
) -> io::Result<()> {
    let mut incoming = Pending::new();
    let mut outgoing = Pending::new();
    let mut remote_eof = false;
    let mut local_eof = false;
    let mut read_shutdown = false;
    let mut write_shutdown = false;
    loop {
        if stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        if !remote_eof && incoming.empty() && !incoming.fill(&mut connection.reader())? {
            remote_eof = true;
        }
        let had_incoming = !incoming.empty();
        incoming.drain(&mut local)?;
        // Flush queued ciphertext before asking rustls to accept more plaintext.
        // A full rustls send buffer reports Ok(0), rather than WouldBlock.
        if !connection.wants_write() {
            outgoing.drain(&mut connection.writer())?;
        }
        if remote_eof && incoming.empty() && !read_shutdown {
            local.shutdown(Shutdown::Write)?;
            read_shutdown = true;
        }
        if local_eof && outgoing.empty() && !write_shutdown {
            connection.send_close_notify();
            write_shutdown = true;
        }
        if local_eof && remote_eof && incoming.empty() && !connection.wants_write() {
            return Ok(());
        }
        if had_incoming && incoming.empty() {
            continue;
        }
        // poll reports HUP even with an empty events mask. Exclude a closed or
        // backpressured read half unless ciphertext still needs to be written.
        let remote_events = if !remote_eof && incoming.empty() {
            libc::POLLIN
        } else {
            0
        } | if connection.wants_write() {
            libc::POLLOUT
        } else {
            0
        };
        let local_events = if !local_eof && outgoing.empty() && !connection.wants_write() {
            libc::POLLIN
        } else {
            0
        } | if !incoming.empty() { libc::POLLOUT } else { 0 };
        let mut descriptors = [
            libc::pollfd {
                fd: if remote_events == 0 {
                    -1
                } else {
                    remote.as_raw_fd()
                },
                events: remote_events,
                revents: 0,
            },
            libc::pollfd {
                fd: if local_events == 0 {
                    -1
                } else {
                    local.as_raw_fd()
                },
                events: local_events,
                revents: 0,
            },
        ];
        before_poll(&descriptors);
        wait(&mut descriptors, -1)?;
        if stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        if descriptors[1].revents & (libc::POLLHUP | libc::POLLERR) != 0 && !incoming.empty() {
            incoming.drain(&mut local)?;
            if !incoming.empty() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native TLS socket closed with pending plaintext",
                ));
            }
        }
        if !local_eof
            && descriptors[1].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0
            && outgoing.empty()
            && !outgoing.fill(&mut local)?
        {
            local_eof = true;
        }
        if descriptors[0].revents & (libc::POLLOUT | libc::POLLHUP | libc::POLLERR) != 0
            && connection.wants_write()
        {
            match connection.write_tls(&mut remote) {
                Ok(_) => {}
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if descriptors[0].revents & libc::POLLERR != 0
            && let Some(error) = remote.take_error()?
        {
            return Err(error);
        }
        if !remote_eof
            && incoming.empty()
            && descriptors[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0
        {
            match connection.read_tls(&mut remote) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "broker TLS stream truncated",
                    ));
                }
                Ok(_) => {
                    connection.process_new_packets().map_err(tls_error)?;
                }
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
#[path = "pump_tests.rs"]
mod tests;
