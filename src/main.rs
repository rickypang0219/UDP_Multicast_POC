use rtrb::{Consumer, Producer, RingBuffer};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, BufRead, BufReader, Write},
    net::{Ipv4Addr, TcpListener, TcpStream, UdpSocket},
    sync::{atomic::{AtomicBool, Ordering}, mpsc::{sync_channel, SyncSender}, Arc},
    thread,
    time::{Duration, Instant},
};

const QUEUE_CAPACITY: usize = 4096;
const MAX_SUBSCRIBERS: usize = 64;
const IDLE_WAIT: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Feed { Trades, Quotes }

impl Feed {
    fn as_str(self) -> &'static str {
        match self { Self::Trades => "trades", Self::Quotes => "quotes" }
    }
}

// Integer prices avoid floating point rounding. ts_ns is Unix time in nanoseconds.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Event { feed: Feed, sequence: u64, price_micros: i64, ts_ns: i64 }

struct Subscriber {
    feeds: Vec<Feed>,
    events: Producer<Event>,
    connected: Arc<AtomicBool>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let upstream = UdpSocket::bind("0.0.0.0:9000")?;
    upstream.set_read_timeout(Some(Duration::from_millis(10)))?;
    // Optional real multicast ingress; default also accepts ordinary UDP.
    if let Ok(group) = std::env::var("MULTICAST_GROUP") {
        upstream.join_multicast_v4(&group.parse()?, &Ipv4Addr::UNSPECIFIED)?;
    }
    let listener = TcpListener::bind("127.0.0.1:9001")?;
    let db_url = std::env::var("QUESTDB_URL")
        .unwrap_or_else(|_| "http://localhost:9000/write?precision=n".into());
    let (log_events, log_reader) = RingBuffer::new(QUEUE_CAPACITY);
    let (registrations, pending_subscribers) = sync_channel(MAX_SUBSCRIBERS);
    thread::spawn(move || write_events_to_questdb(log_reader, db_url));
    thread::spawn(move || accept_subscribers(listener, registrations));
    eprintln!("UDP :9000 -> TCP 127.0.0.1:9001 and QuestDB; SUB trades quotes");
    dispatch_events(upstream, pending_subscribers, log_events)?;
    Ok(())
}

fn accept_subscribers(listener: TcpListener, registrations: SyncSender<Subscriber>) {
    // Reserve a slot before spawning: slow handshakes cannot create unlimited threads.
    let slots: Vec<_> = (0..MAX_SUBSCRIBERS).map(|_| Arc::new(AtomicBool::new(false))).collect();
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let Some(slot) = slots.iter().find(|slot| !slot.swap(true, Ordering::AcqRel)) else {
            drop(stream);
            continue;
        };
        let slot = Arc::clone(slot);
        let registrations = registrations.clone();
        thread::spawn(move || {
            if let Err(error) = serve_subscriber(stream, registrations) {
                eprintln!("subscriber disconnected: {error}");
            }
            slot.store(false, Ordering::Release);
        });
    }
}

fn serve_subscriber(mut stream: TcpStream, registrations: SyncSender<Subscriber>) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.set_nodelay(true)?;
    let mut line = String::new();
    // Read at most 129 bytes; reject overlong commands without unbounded allocation.
    use std::io::Read;
    BufReader::new((&stream).take(129)).read_line(&mut line)?;
    let feeds = parse_subscription(&line)?;
    let (events, mut reader) = RingBuffer::new(QUEUE_CAPACITY);
    let connected = Arc::new(AtomicBool::new(true));
    registrations.try_send(Subscriber { feeds, events, connected: connected.clone() })
        .map_err(|_| io::Error::other("registration queue full"))?;
    let result = (|| {
        stream.write_all(b"OK\n")?;
        loop {
            match reader.pop() {
                Ok(event) => {
                    serde_json::to_writer(&mut stream, &event)?;
                    stream.write_all(b"\n")?;
                }
                Err(_) if reader.is_abandoned() => return Ok(()),
                Err(_) => {
                    // Detect idle client disconnects without consuming further commands.
                    stream.set_read_timeout(Some(IDLE_WAIT))?;
                    let mut byte = [0];
                    match stream.peek(&mut byte) {
                        Ok(0) => return Ok(()),
                        Ok(_) => return Err(io::Error::other("only one SUB command allowed")),
                        Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {},
                        Err(e) => return Err(e),
                    }
                    thread::sleep(IDLE_WAIT);
                }
            }
        }
    })();
    connected.store(false, Ordering::Release);
    result
}

fn parse_subscription(line: &str) -> io::Result<Vec<Feed>> {
    let invalid = || io::Error::other("expected SUB trades and/or quotes, newline terminated, <=128 bytes");
    if line.len() > 128 || !line.ends_with('\n') { return Err(invalid()); }
    let mut words = line.split_whitespace();
    if words.next() != Some("SUB") { return Err(invalid()); }
    let mut feeds = Vec::new();
    for word in words {
        let feed = match word { "trades" => Feed::Trades, "quotes" => Feed::Quotes, _ => return Err(invalid()) };
        if !feeds.contains(&feed) { feeds.push(feed); }
    }
    if feeds.is_empty() { return Err(invalid()); }
    Ok(feeds)
}

fn dispatch_events(socket: UdpSocket, registrations: std::sync::mpsc::Receiver<Subscriber>, mut logs: Producer<Event>) -> io::Result<()> {
    let mut subscribers: Vec<Subscriber> = Vec::new();
    let mut packet = [0; 65535];
    let (mut dropped_logs, mut dropped_deliveries, mut invalid_packets) = (0u64, 0u64, 0u64);
    let mut last_report = Instant::now();
    loop {
        subscribers.retain(|s| s.connected.load(Ordering::Acquire));
        subscribers.extend(registrations.try_iter());
        match socket.recv(&mut packet) {
            Ok(size) => match serde_json::from_slice::<Event>(&packet[..size]) {
                Ok(event) if event.sequence <= i64::MAX as u64 && event.ts_ns >= 0 => {
                    if logs.push(event).is_err() { dropped_logs += 1; }
                    for subscriber in &mut subscribers {
                        if subscriber.feeds.contains(&event.feed) && subscriber.events.push(event).is_err() {
                            dropped_deliveries += 1;
                        }
                    }
                }
                _ => invalid_packets += 1,
            },
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {},
            Err(e) => return Err(e),
        }
        if last_report.elapsed() >= Duration::from_secs(5) {
            eprintln!("subscribers={} dropped_logs={dropped_logs} dropped_deliveries={dropped_deliveries} invalid_packets={invalid_packets}", subscribers.len());
            last_report = Instant::now();
        }
    }
}

fn write_events_to_questdb(mut events: Consumer<Event>, url: String) {
    let client = ureq::AgentBuilder::new().timeout(Duration::from_secs(3)).build();
    loop {
        let mut batch = String::new();
        let deadline = Instant::now() + Duration::from_millis(100);
        let mut count = 0;
        while count < 256 && Instant::now() < deadline {
            match events.pop() {
                Ok(event) => {
                    use std::fmt::Write;
                    writeln!(batch, "events,feed={} sequence={}i,price_micros={}i {}", event.feed.as_str(), event.sequence, event.price_micros, event.ts_ns).unwrap();
                    count += 1;
                }
                Err(_) => thread::sleep(IDLE_WAIT),
            }
        }
        if batch.is_empty() { continue; }
        // Retain the same bounded batch during failure; ingress never waits for DB.
        while let Err(error) = client.post(&url).set("Content-Type", "text/plain; charset=utf-8").send_string(&batch) {
            eprintln!("QuestDB write failed; retry in 1s: {error}");
            thread::sleep(Duration::from_secs(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn subscription_rejects_unknown_feeds_and_deduplicates() {
        assert_eq!(parse_subscription("SUB trades quotes trades\n").unwrap(), vec![Feed::Trades, Feed::Quotes]);
        for bad in ["SUB\n", "SUB unknown\n", "SUB trades", "GET trades\n"] {
            assert!(parse_subscription(bad).is_err());
        }
    }
    #[test]
    fn full_queue_drops_new_event_and_preserves_fifo() {
        let (mut producer, mut consumer) = RingBuffer::new(1);
        producer.push(1).unwrap();
        assert!(producer.push(2).is_err());
        assert_eq!(consumer.pop().unwrap(), 1);
    }
}
