mod common;
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn test_simple_select() {
    let addr = common::clickhouse_addr().to_owned();
    let result = tokio::task::spawn_blocking(move || {
        use std::net::TcpStream;
        use std::io::{Read, Write};

        let mut s = TcpStream::connect(addr).expect("test operation failed");
        s.set_nodelay(true).expect("test operation failed");
        s.set_read_timeout(Some(Duration::from_secs(5))).expect("test operation failed");
        eprintln!("Connected");

        // Hello
        let hello = b"\x00\x11ClickHouse client\x1a\x04\xd3\xa9\x03\x00\x07default\x04test";
        s.write_all(hello).expect("test operation failed");
        s.flush().expect("test operation failed");
        eprintln!("Hello sent ({}b)", hello.len());

        std::thread::sleep(Duration::from_millis(300));

        let mut buf = vec![0u8; 4096];
        let n = s.read(&mut buf).expect("test operation failed");
        eprintln!("Hello recv: {}b", n);
        if n == 0 { eprintln!("CLOSED"); return; }

        // Addendum + chunked
        let a = b"\x00\x0anotchunked\x0anotchunked\x07";
        s.write_all(a).expect("test operation failed");
        eprintln!("Addendum+chunked sent ({}b)", a.len());

        // Ping
        s.write_all(b"\x04").expect("test operation failed");
        std::thread::sleep(Duration::from_millis(100));
        let n = s.read(&mut buf).expect("test operation failed");
        eprintln!("Pong: {}b type={}", n, buf[0]);

        // Query (EXACT 101 bytes)
        let q = b"\x01\x00\x01\x00\x00\x090.0.0.0:0\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x0ccedb6ede9cbf\x11ClickHouse client\x1a\x04\xd3\xa9\x03\x00\x00\x02\x00\x00\x00\x00\x01\x01\x00\x00\x01\x00\x00\x02\x00\x08SELECT 1\x00\x02\x00\x01\x00\x02\xff\xff\xff\xff\x03\x00\x00\x00\x00";
        s.write_all(q).expect("test operation failed");
        eprintln!("Query sent ({}b)", q.len());

        std::thread::sleep(Duration::from_millis(200));
        let n = s.read(&mut buf).expect("test operation failed");
        if n > 0 {
            eprintln!("Response: {}b type={}", n, buf[0]);
        } else {
            eprintln!("CLOSED");
        }
    }).await;

    result.expect("test operation failed");
}
