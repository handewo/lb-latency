use log::{debug, error};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration, Instant};

// header "GET / HTTP/1.0\nHOST: 127.0.0.1\n\n"
static REQ_HEADER: &[u8] = &[
    71u8, 69u8, 84u8, 32u8, 47u8, 32u8, 72u8, 84u8, 84u8, 80u8, 47u8, 49u8, 46u8, 48u8, 10u8, 72u8,
    79u8, 83u8, 84u8, 58u8, 32u8, 49u8, 50u8, 55u8, 46u8, 48u8, 46u8, 48u8, 46u8, 49u8, 10u8, 10u8,
];

//response "HTTP/1.1 200 OK"
static RESP_EXPECT: &[u8] = &[
    72u8, 84u8, 84u8, 80u8, 47u8, 49u8, 46u8, 49u8, 32u8, 50u8, 48u8, 48u8, 32u8, 79u8, 75u8,
];

pub struct Proxy {
    pub frontend: u16,
    pub index: AtomicUsize,
    pub backend_addrs: Vec<Arc<String>>,
}

impl Proxy {
    pub fn fatest_backend(&self) -> Arc<String> {
        Arc::clone(&self.backend_addrs[self.index.load(Ordering::SeqCst)])
    }

    pub async fn check_latency(&self, check_timout: u64) {
        let mut tasks = Vec::with_capacity(self.backend_addrs.len());
        for addr in self.backend_addrs.iter() {
            let addr = Arc::clone(addr);
            tasks.push(tokio::spawn(timeout(
                Duration::from_secs(check_timout),
                async move {
                    let start = Instant::now();
                    let mut stream = match TcpStream::connect(addr.as_str()).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("check failed when build connection with {addr}: {e}");
                            return u64::MAX;
                        }
                    };
                    stream
                        .write_all(REQ_HEADER)
                        .await
                        .unwrap_or_else(|e| error!("check falied when request {addr}: {e}"));
                    let mut buf = vec![0; 256];
                    if let Err(e) = stream.read(&mut buf).await {
                        error!("read error when check {addr}: {e}");
                        return u64::MAX;
                    };
                    let duration = start.elapsed().as_micros() as u64;
                    debug!("{duration}us is elapsed when trying to connect to {addr}");
                    if &buf[..15] == RESP_EXPECT {
                        debug!("response from {addr} is expected");
                        return duration;
                    }
                    u64::MAX
                },
            )));
        }
        let mut outputs = Vec::with_capacity(tasks.len());
        for task in tasks {
            outputs.push(task.await.unwrap());
        }

        let mut res = Vec::new();
        for (i, d) in outputs.into_iter().enumerate() {
            let v = d.unwrap_or_else(|_| {
                debug!("check backend: {} timeout", self.backend_addrs[i]);
                u64::MAX
            });
            res.push((i, v))
        }
        let (i, d) = res.iter().min_by_key(|x| x.1).unwrap();

        if *d != u64::MAX && *i != self.index.load(Ordering::SeqCst) {
            self.index.store(*i, Ordering::SeqCst);
            debug!("{} switch to faster backend {i}", self.frontend);
        }
    }
}

pub async fn process(mut socket: TcpStream, proxy: Arc<Proxy>) {
    let mut s_buf = vec![0; 1024];
    let mut b_buf = vec![0; 1024];
    let back_addr = proxy.fatest_backend();

    let mut back_stream = match TcpStream::connect(back_addr.as_str()).await {
        Ok(s) => s,
        Err(e) => {
            error!("process failed when build connection with {back_addr}: {e}");
            return;
        }
    };
    debug!("connected to backend {back_addr}");

    loop {
        tokio::select! {
            Ok(n) = socket.read(&mut s_buf) => {
                if n == 0 {
                    debug!("client has reached EOF");
                    break;
                }
                debug!("read data from client");
                if let Err(e) = back_stream.write(&s_buf[..n]).await {
                    error!("write data from client to backend error: {e}");
                    break;
                }
            }
            Ok(n) = back_stream.read(&mut b_buf) => {
                if n == 0 {
                    debug!("backend has reached EOF");
                    break;
                }
                debug!("read data from backend");
                if let Err(e) = socket.write(&b_buf[..n]).await {
                    error!("write data from back_end to client error: {e}");
                    break;
                }
            }
            else => {
                error!("client or backend read error");
                break
            }
        }
    }
}
