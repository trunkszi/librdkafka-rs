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
    loop {
        // SAFETY: poll receives the valid mutable slice and its exact length.
        let result =
            unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, timeout) };
        if result > 0 {
            if descriptors
                .iter()
                .any(|fd| fd.revents & libc::POLLNVAL != 0)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "closed TLS descriptor",
                ));
            }
            return Ok(());
        }
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "broker TLS handshake timed out",
            ));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(super) fn handshake(
    connection: &mut rustls::ClientConnection,
    mut remote: &TcpStream,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
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
            remaining.as_millis().min(i32::MAX as u128) as i32,
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
    mut connection: rustls::ClientConnection,
    mut remote: TcpStream,
    mut local: &UnixStream,
    stopping: &AtomicBool,
) -> io::Result<()> {
    let mut incoming = Pending::new();
    let mut outgoing = Pending::new();
    let mut peer_closed = false;
    loop {
        if stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        if incoming.empty() && !incoming.fill(&mut connection.reader())? {
            peer_closed = true;
        }
        let had_incoming = !incoming.empty();
        incoming.drain(&mut local)?;
        outgoing.drain(&mut connection.writer())?;
        if peer_closed && incoming.empty() {
            local.shutdown(Shutdown::Write)?;
            return Ok(());
        }
        if had_incoming && incoming.empty() {
            continue;
        }
        let mut descriptors = [
            libc::pollfd {
                fd: remote.as_raw_fd(),
                events: if incoming.empty() { libc::POLLIN } else { 0 }
                    | if connection.wants_write() {
                        libc::POLLOUT
                    } else {
                        0
                    },
                revents: 0,
            },
            libc::pollfd {
                fd: local.as_raw_fd(),
                events: if outgoing.empty() && !connection.wants_write() {
                    libc::POLLIN
                } else {
                    0
                } | if !incoming.empty() { libc::POLLOUT } else { 0 },
                revents: 0,
            },
        ];
        wait(&mut descriptors, -1)?;
        if descriptors[1].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0
            && outgoing.empty()
            && !outgoing.fill(&mut local)?
        {
            connection.send_close_notify();
            match connection.write_tls(&mut remote) {
                Ok(_) => {}
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
            return Ok(());
        }
        if descriptors[0].revents & libc::POLLOUT != 0 {
            match connection.write_tls(&mut remote) {
                Ok(_) => {}
                Err(error) if would_block(&error) => {}
                Err(error) => return Err(error),
            }
        }
        if incoming.empty()
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
