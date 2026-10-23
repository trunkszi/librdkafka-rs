//! Deadline-bounded c-ares resolution for TLS peer-name overrides.
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::{Duration, Instant},
};

use nexora::{ClientConfig, HostResolver, Proactor, SubsystemInbox, inbox::InboxCommand};

use crate::{Error, Result};

pub(super) fn resolve(host: &str, port: u16, deadline: Instant) -> Result<Vec<IpAddr>> {
    if Instant::now() >= deadline {
        return Err(Error::Http("HTTP request timed out".into()));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![ip]);
    }
    if is_localhost(host) {
        return Ok(vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ]);
    }
    Resolver::new(None)?.resolve(host, port, deadline)
}

pub(super) fn is_localhost(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    host == "localhost" || host.ends_with(".localhost")
}

struct Resolver {
    resolver: Option<HostResolver>,
    ring: Proactor,
    retired: bool,
}

impl Resolver {
    fn new(servers: Option<&str>) -> Result<Self> {
        let subsystem = ClientConfig::default().dns_subsystem;
        let resolver = match servers {
            Some(servers) => HostResolver::new(subsystem, Default::default(), servers, 64),
            None => HostResolver::default_resolver(subsystem),
        }
        .map_err(super::transport_error)?;
        let ring = Proactor::builder()
            .build()
            .map_err(|_| Error::Http("event-loop initialization failed".into()))?;
        Ok(Self {
            resolver: Some(resolver),
            ring,
            retired: false,
        })
    }

    fn resolve(&mut self, host: &str, port: u16, deadline: Instant) -> Result<Vec<IpAddr>> {
        let inbox = SubsystemInbox::new();
        let resolver = self
            .resolver
            .as_mut()
            .ok_or_else(|| Error::Http("HTTP DNS driver unavailable".into()))?;
        let handles = resolver
            .start_query(&mut self.ring, host, port, &inbox)
            .map_err(super::transport_error)?;
        let result = (|| {
            let mut answers = 0;
            let mut addresses = Vec::new();
            loop {
                if Instant::now() >= deadline {
                    return Err(Error::Http("HTTP request timed out".into()));
                }
                while let Some(command) = inbox.pop() {
                    if let InboxCommand::DnsResolved(answer) = command {
                        answers += 1;
                        for address in answer.addrs {
                            if !addresses.contains(&address.ip()) {
                                addresses.push(address.ip());
                            }
                        }
                    }
                }
                if answers == 2 {
                    if addresses.is_empty() {
                        return Err(Error::Http("HTTP endpoint DNS lookup failed".into()));
                    }
                    addresses.sort_unstable();
                    return Ok(addresses);
                }
                advance(resolver, &mut self.ring, deadline)?;
            }
        })();
        resolver.cancel_queries(&handles);
        result
    }

    fn retire(&mut self) -> Result<()> {
        if self.retired {
            return Ok(());
        }
        if let Some(resolver) = self.resolver.as_mut() {
            resolver
                .begin_shutdown(&mut self.ring)
                .map_err(super::transport_error)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            while resolver.has_pending_io() {
                if Instant::now() >= deadline {
                    return Err(Error::Http("HTTP DNS shutdown timed out".into()));
                }
                advance(resolver, &mut self.ring, deadline)?;
            }
        }
        self.retired = true;
        Ok(())
    }
}

fn advance(resolver: &mut HostResolver, ring: &mut Proactor, deadline: Instant) -> Result<()> {
    resolver.flush(ring).map_err(super::transport_error)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::Http("HTTP request timed out".into()));
    }
    ring.wait_completions(1, Some(remaining.min(Duration::from_millis(20))), resolver)
        .map_err(|_| Error::Http("HTTP DNS completion failed".into()))?;
    Ok(())
}

impl Drop for Resolver {
    fn drop(&mut self) {
        let retirement = self.retire();
        let shutdown = self.ring.shutdown();
        if retirement.is_err() || shutdown.is_err() {
            // Failed retirement never permits releasing kernel-referenced storage.
            std::mem::forget(self.resolver.take());
        }
        if let Err(error) = retirement {
            eprintln!("HTTP DNS retirement failed: {error}");
        }
        if let Err(error) = shutdown {
            eprintln!("HTTP DNS shutdown failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::UdpSocket, time::Duration};

    use super::*;

    #[test]
    fn unit_scenario_localhost_names_stay_local_and_respect_name_boundaries() {
        for host in ["localhost", "LOCALHOST.", "service.localhost"] {
            assert!(is_localhost(host));
            let addresses = resolve(host, 443, Instant::now() + Duration::from_secs(1)).unwrap();
            assert_eq!(
                addresses,
                [
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V6(Ipv6Addr::LOCALHOST)
                ]
            );
        }
        for host in ["notlocalhost", "localhost.example", "localhost.."] {
            assert!(!is_localhost(host));
        }
    }

    #[test]
    fn integration_lifecycle_scenario_unresponsive_dns_obeys_request_deadline() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let address = peer.local_addr().unwrap();
        let mut resolver = Resolver::new(Some(&address.to_string())).unwrap();
        let started = Instant::now();
        let error = resolver
            .resolve(
                "timeout.fixture.test",
                443,
                started + Duration::from_millis(100),
            )
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
        let mut query = [0; 512];
        assert!(peer.recv_from(&mut query).unwrap().0 > 12);
        resolver.retire().unwrap();
        assert!(!resolver.resolver.as_ref().unwrap().has_pending_io());
        drop(resolver);
    }

    #[test]
    fn integration_scenario_dns_answer_preserves_original_endpoint_addresses() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let address = peer.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut query = [0; 512];
            for _ in 0..2 {
                let (length, remote) = peer.recv_from(&mut query).unwrap();
                let mut end = 12;
                while query[end] != 0 {
                    end += usize::from(query[end]) + 1;
                }
                end += 5;
                assert!(end <= length);
                let is_ipv4 = query[end - 4..end - 2] == [0, 1];
                let mut response = vec![
                    query[0],
                    query[1],
                    0x81,
                    0x80,
                    0,
                    1,
                    0,
                    u8::from(is_ipv4),
                    0,
                    0,
                    0,
                    0,
                ];
                response.extend_from_slice(&query[12..end]);
                if is_ipv4 {
                    response.extend_from_slice(&[
                        0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 127, 0, 0, 1,
                    ]);
                }
                peer.send_to(&response, remote).unwrap();
            }
        });
        let mut resolver = Resolver::new(Some(&address.to_string())).unwrap();
        let addresses = resolver
            .resolve(
                "original.fixture.test",
                443,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
        assert_eq!(addresses, vec!["127.0.0.1".parse::<IpAddr>().unwrap()]);
        resolver.retire().unwrap();
        assert!(!resolver.resolver.as_ref().unwrap().has_pending_io());
        drop(resolver);
        server.join().unwrap();
    }
}
