use std::{
    net::TcpListener,
    sync::{Arc, atomic::AtomicUsize, mpsc},
    thread::JoinHandle,
};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

fn connected(
    serve: impl FnOnce(Server) -> TestResult + Send + 'static,
) -> (rustls::ClientConnection, TcpStream, JoinHandle<TestResult>) {
    let identity = rcgen::generate_simple_self_signed(vec!["broker.test".into()]).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(identity.key_pair.serialize_der()).into(),
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(identity.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        socket.set_write_timeout(Some(Duration::from_secs(5)))?;
        let mut stream = Server::new(rustls::ServerConnection::new(Arc::new(server))?, socket);
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        serve(stream)
    });
    let remote = TcpStream::connect(address).unwrap();
    remote.set_nonblocking(true).unwrap();
    let mut connection =
        rustls::ClientConnection::new(Arc::new(client), "broker.test".try_into().unwrap()).unwrap();
    connection.set_buffer_limit(Some(BUFFER_SIZE));
    handshake(&mut connection, &remote, Duration::from_secs(5)).unwrap();
    (connection, remote, worker)
}

fn small_send_buffer(socket: &impl AsRawFd) {
    let size: libc::c_int = 4096;
    assert_eq!(
        // SAFETY: size is a valid integer socket option with its exact byte length.
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as _,
            )
        },
        0
    );
}

#[test]
fn integration_lifecycle_scenario_peer_close_blocks_poll_under_local_backpressure() {
    let payload = vec![73; 256 * 1024];
    let sent = payload.clone();
    let (mut connection, mut remote, server) = connected(move |mut stream| {
        stream.write_all(&sent)?;
        let mut client_payload = Vec::new();
        stream.read_to_end(&mut client_payload)?;
        assert!(client_payload.is_empty());
        stream.conn.send_close_notify();
        stream.flush()?;
        Ok(())
    });
    // Finish the TCP write half so the peer's FIN produces POLLHUP, including
    // when poll requests no events for its backpressured read half.
    connection.send_close_notify();
    while connection.wants_write() {
        connection.write_tls(&mut remote).unwrap();
    }
    remote.shutdown(Shutdown::Write).unwrap();
    let remote_observer = remote.try_clone().unwrap();
    let (local, mut native) = UnixStream::pair().unwrap();
    small_send_buffer(&local);
    local.set_nonblocking(true).unwrap();
    native
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut announced = false;
        run_loop(connection, remote, &local, &AtomicBool::new(false), |fds| {
            observed.fetch_add(1, Ordering::Relaxed);
            if !announced && fds[0].fd == -1 && fds[1].events & libc::POLLOUT != 0 {
                announced = true;
                blocked_tx.send(()).unwrap();
            }
        })
    });
    blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    server.join().unwrap().unwrap();
    let mut closed = libc::pollfd {
        fd: remote_observer.as_raw_fd(),
        events: 0,
        revents: 0,
    };
    // SAFETY: closed points to one initialized poll descriptor.
    assert_eq!(unsafe { libc::poll(&mut closed, 1, 0) }, 1);
    assert_ne!(closed.revents & libc::POLLHUP, 0);
    let baseline = count.load(Ordering::Relaxed);
    // No local reader can create write readiness during this observation window.
    assert!(matches!(
        blocked_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(
        count.load(Ordering::Relaxed) <= baseline + 1,
        "closed TCP peer repeatedly woke a backpressured pump"
    );
    let mut received = Vec::new();
    native.read_to_end(&mut received).unwrap();
    assert_eq!(received, payload);
    native.shutdown(Shutdown::Write).unwrap();
    worker.join().unwrap().unwrap();
}

#[test]
fn integration_lifecycle_scenario_local_halfclose_flushes_ciphertext_and_clean_notify() {
    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    let (release, resume) = mpsc::channel();
    let (connection, remote, server) = connected(move |mut stream| {
        resume.recv_timeout(Duration::from_secs(5))?;
        let mut received = Vec::new();
        // rustls returns UnexpectedEof if ciphertext or close_notify is lost.
        stream.read_to_end(&mut received)?;
        assert_eq!(received, expected);
        stream.write_all(b"accepted after halfclose")?;
        stream.conn.send_close_notify();
        stream.flush()?;
        Ok(())
    });
    small_send_buffer(&remote);
    let (local, mut native) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    native
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    native
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (pending_tx, pending_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut announced = false;
        run_loop(connection, remote, &local, &AtomicBool::new(false), |fds| {
            if !announced && fds[0].events & libc::POLLOUT != 0 && fds[1].fd == -1 {
                announced = true;
                pending_tx.send(()).unwrap();
            }
        })
    });
    let mut writer = native.try_clone().unwrap();
    let producer = std::thread::spawn(move || {
        writer.write_all(&payload).unwrap();
        writer.shutdown(Shutdown::Write).unwrap();
    });
    pending_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    producer.join().unwrap();
    let mut response = Vec::new();
    native.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"accepted after halfclose");
    worker.join().unwrap().unwrap();
    server.join().unwrap().unwrap();
}

#[test]
fn integration_lifecycle_scenario_stop_wakes_poll_after_local_halfclose() {
    let (release, resume) = mpsc::channel();
    let (connection, remote, server) = connected(move |_stream| {
        resume.recv_timeout(Duration::from_secs(5))?;
        Ok(())
    });
    let (local, native) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    let local_wake = local.try_clone().unwrap();
    let remote_wake = remote.try_clone().unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let worker_stopping = stopping.clone();
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    native.shutdown(Shutdown::Write).unwrap();
    let worker = std::thread::spawn(move || {
        let mut announced = false;
        let result = run_loop(connection, remote, &local, &worker_stopping, |fds| {
            if !announced && fds[1].fd == -1 && fds[0].events == libc::POLLIN {
                announced = true;
                waiting_tx.send(()).unwrap();
            }
        });
        finished_tx.send(()).unwrap();
        result
    });
    waiting_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    stopping.store(true, Ordering::Release);
    local_wake.shutdown(Shutdown::Both).unwrap();
    remote_wake.shutdown(Shutdown::Both).unwrap();
    finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.join().unwrap().unwrap();
    release.send(()).unwrap();
    server.join().unwrap().unwrap();
}

#[test]
fn integration_lifecycle_scenario_small_tls_buffer_flushes_large_plaintext() {
    let payload = vec![47; BUFFER_SIZE * 4 + 17];
    let expected = payload.clone();
    let (mut connection, remote, server) = connected(move |mut stream| {
        let mut received = Vec::new();
        stream.read_to_end(&mut received)?;
        assert_eq!(received, expected);
        stream.conn.send_close_notify();
        stream.flush()?;
        Ok(())
    });
    connection.set_buffer_limit(Some(1024));
    small_send_buffer(&remote);
    let (local, mut native) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    native
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    native
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let worker =
        std::thread::spawn(move || run(connection, remote, &local, &AtomicBool::new(false)));
    native.write_all(&payload).unwrap();
    native.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    native.read_to_end(&mut response).unwrap();
    assert!(response.is_empty());
    worker.join().unwrap().unwrap();
    server.join().unwrap().unwrap();
}

#[test]
fn integration_lifecycle_scenario_remote_reset_breaks_bidirectional_backpressure() {
    let (reset_tx, reset_rx) = mpsc::channel();
    let (connection, remote, server) = connected(move |mut stream| {
        stream.write_all(&vec![29; BUFFER_SIZE * 2])?;
        reset_rx.recv_timeout(Duration::from_secs(5))?;
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: linger is a valid socket option with its exact byte length.
        let result = unsafe {
            libc::setsockopt(
                stream.sock.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of_val(&linger) as _,
            )
        };
        assert_eq!(result, 0);
        Ok(())
    });
    small_send_buffer(&remote);
    let (local, mut native) = UnixStream::pair().unwrap();
    small_send_buffer(&local);
    local.set_nonblocking(true).unwrap();
    native
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let worker = std::thread::spawn(move || {
        let mut announced = false;
        let outcome = run_loop(connection, remote, &local, &AtomicBool::new(false), |fds| {
            observed.fetch_add(1, Ordering::Relaxed);
            if !announced
                && fds[0].events & libc::POLLOUT != 0
                && fds[1].events & libc::POLLOUT != 0
            {
                announced = true;
                blocked_tx.send(()).unwrap();
            }
        });
        done_tx.send(()).unwrap();
        outcome
    });
    let writer = std::thread::spawn(move || native.write_all(&vec![83; BUFFER_SIZE * 32]));
    blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    reset_tx.send(()).unwrap();
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        worker.join().unwrap().is_err(),
        "reset must fail the transport"
    );
    let _writer_outcome = writer.join().unwrap();
    server.join().unwrap().unwrap();
    assert!(
        polls.load(Ordering::Relaxed) < 1024,
        "reset repeatedly woke the pump"
    );
}

#[test]
fn unit_scenario_handshake_timeout_overflow_returns_error() {
    let (mut connection, remote, server) = connected(|_| Ok(()));
    let error = handshake(&mut connection, &remote, Duration::MAX).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    server.join().unwrap().unwrap();
}

#[test]
fn integration_lifecycle_scenario_native_close_releases_pending_plaintext() {
    let (release_tx, release_rx) = mpsc::channel();
    let (connection, remote, server) = connected(move |mut stream| {
        stream.write_all(&vec![57; BUFFER_SIZE * 2])?;
        release_rx.recv_timeout(Duration::from_secs(5))?;
        Ok(())
    });
    let (local, native) = UnixStream::pair().unwrap();
    small_send_buffer(&local);
    local.set_nonblocking(true).unwrap();
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut announced = false;
        let mut polls = 0;
        let result = run_loop(connection, remote, &local, &AtomicBool::new(false), |fds| {
            polls += 1;
            if !announced && fds[1].events & libc::POLLOUT != 0 {
                announced = true;
                blocked_tx.send(()).unwrap();
            }
        });
        done_tx.send(()).unwrap();
        (result, polls)
    });
    blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(native);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (outcome, polls) = worker.join().unwrap();
    assert!(
        outcome.is_err(),
        "native close must release pending plaintext"
    );
    assert!(polls < 1024, "native HUP repeatedly woke the pump");
    release_tx.send(()).unwrap();
    server.join().unwrap().unwrap();
}

#[test]
fn unit_scenario_interrupted_wait_returns_to_absolute_deadline() {
    let mut descriptors = [libc::pollfd {
        fd: -1,
        events: libc::POLLIN,
        revents: 0,
    }];
    let deadline_ms = 20;
    let mut elapsed_ms = 0;
    let mut requested_timeouts = Vec::new();
    while elapsed_ms < deadline_ms {
        wait_with(
            &mut descriptors,
            deadline_ms - elapsed_ms,
            |fds, timeout| {
                requested_timeouts.push(timeout);
                elapsed_ms += 5;
                // Interrupted poll must not leave stale readiness for the caller.
                fds[0].revents = libc::POLLIN;
                Err(io::ErrorKind::Interrupted.into())
            },
        )
        .unwrap();
        assert_eq!(descriptors[0].revents, 0);
    }
    assert_eq!(requested_timeouts, [20, 15, 10, 5]);
    assert_eq!(elapsed_ms, deadline_ms);
    let error = wait_with(&mut descriptors, 0, |_, _| Ok(0)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let error = wait_with(&mut descriptors, -1, |_, _| {
        Err(io::ErrorKind::BrokenPipe.into())
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}
