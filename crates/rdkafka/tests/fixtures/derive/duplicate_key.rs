use rdkafka::Record;

#[derive(Record)]
struct Event {
    #[record(key)]
    first: String,
    #[record(key)]
    second: String,
}

fn main() {}
