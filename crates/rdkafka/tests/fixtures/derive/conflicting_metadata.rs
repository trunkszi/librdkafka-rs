use rdkafka::Record;

#[derive(Record)]
struct Event {
    #[record(key, timestamp)]
    value: i64,
}

fn main() {}
