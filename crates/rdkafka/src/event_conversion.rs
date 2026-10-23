//! Structured projections retain native callback evidence and units.
use std::time::Duration;

use rdkafka_sys::ffi;

use super::{DeliveryReport, Event, MsgStatus};
impl DeliveryReport {
    pub(crate) fn from_native(event: &ffi::NativeEvent) -> Self {
        Self {
            topic: event.topic.clone(),
            partition: event.partition,
            offset: event.offset,
            token: event.token,
            error: (event.code != 0).then_some(crate::ErrorCode(event.code)),
            status: MsgStatus::from_raw(event.status),
            latency: u64::try_from(event.latency_us)
                .ok()
                .map(Duration::from_micros),
        }
    }
}
impl Event {
    pub(crate) fn from_native(event: &ffi::NativeEvent) -> Self {
        match event.kind {
            2 => Self::Stats(event.message.clone()),
            3 => Self::Log {
                level: event.log_level,
                fac: event.log_facility.clone(),
                message: event.message.clone(),
            },
            4 => Self::Throttle {
                broker_name: event.broker_name.clone(),
                broker_id: event.broker_id,
                throttle_time: Duration::from_millis(event.throttle_ms.max(0) as u64),
            },
            _ => Self::Error {
                code: event.code,
                message: event.message.clone(),
                is_fatal: event.fatal,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event() -> ffi::NativeEvent {
        ffi::NativeEvent {
            kind: 0,
            code: 0,
            message: String::new(),
            topic: String::new(),
            partition: -1,
            offset: -1,
            token: 0,
            partitions: Vec::new(),
            status: -1,
            latency_us: -1,
            broker_name: String::new(),
            broker_id: -1,
            throttle_ms: 0,
            log_level: 0,
            log_facility: String::new(),
            fatal: false,
            timestamp: -1,
            key: Vec::new(),
            has_key: false,
            payload: Vec::new(),
            has_payload: false,
            headers: Vec::new(),
            assignment_lost: false,
            rebalance_protocol: String::new(),
        }
    }
    #[test]
    fn unit_scenario_throttle_log_and_fatal_evidence_remain_structured() {
        let mut native = event();
        native.kind = 4;
        native.broker_name = "broker-17".into();
        native.broker_id = 17;
        native.throttle_ms = 23;
        assert!(matches!(Event::from_native(&native), Event::Throttle {
            broker_name, broker_id: 17, throttle_time
        } if broker_name == "broker-17" && throttle_time == Duration::from_millis(23)));
        native.kind = 3;
        native.log_level = 6;
        native.log_facility = "CONNECT".into();
        native.message = "connection established".into();
        assert!(matches!(Event::from_native(&native), Event::Log {
            level: 6, fac, message
        } if fac == "CONNECT" && message == "connection established"));
        native.kind = 1;
        native.code = -144;
        native.fatal = true;
        assert!(matches!(
            Event::from_native(&native),
            Event::Error {
                code: -144,
                is_fatal: true,
                ..
            }
        ));
    }
    #[test]
    fn unit_scenario_unavailable_and_nonpersisted_deliveries() {
        let mut native = event();
        native.status = 1;
        native.code = -192;
        let report = DeliveryReport::from_native(&native);
        assert_eq!(report.status, MsgStatus::PossiblyPersisted);
        assert_eq!(report.error, Some(crate::ErrorCode::MSG_TIMED_OUT));
        assert_eq!(report.latency, None);
        native.status = 0;
        native.latency_us = 1234;
        let report = DeliveryReport::from_native(&native);
        assert_eq!(report.status, MsgStatus::NotPersisted);
        assert_eq!(report.latency, Some(Duration::from_micros(1234)));
    }
}
