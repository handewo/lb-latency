use log::{debug, error, trace, warn};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration, Instant};

// header "GET / HTTP/1.0\nHOST: 127.0.0.1\n\n"
static REQ_HEADER: &[u8] = b"GET / HTTP/1.0\nHOST: 127.0.0.1\n\n";

//response "HTTP/1.1 200 OK"
static RESP_EXPECT: &[u8] = b"HTTP/1.1 200 OK";

const BUF_LENGTH: usize = 65536;

pub struct Proxy {
    pub frontend: String,
    pub index: AtomicUsize,
    pub backend_addrs: Vec<Arc<String>>,
    pub back_traffic: Vec<Arc<Traffic>>,
    pub last_latency: Vec<AtomicU64>,
}

pub struct Traffic {
    pub send: AtomicU64,
    pub recv: AtomicU64,
}

impl Proxy {
    pub fn fatest_backend(&self) -> (Arc<Traffic>, Arc<String>) {
        let index = self.index.load(Ordering::SeqCst);

        (
            Arc::clone(&self.back_traffic[index]),
            Arc::clone(&self.backend_addrs[index]),
        )
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
                            warn!("check failed when build connection with {addr}: {e}");
                            return u64::MAX;
                        }
                    };
                    stream
                        .write_all(REQ_HEADER)
                        .await
                        .unwrap_or_else(|e| warn!("check falied when send request to {addr}: {e}"));
                    let mut buf = vec![0; 256];
                    if let Err(e) = stream.read(&mut buf).await {
                        warn!("read error when check {addr}: {e}");
                        return u64::MAX;
                    };
                    let duration = start.elapsed().as_micros() as u64;
                    trace!("{duration}us is elapsed when trying to connect to {addr}");
                    if &buf[..15] == RESP_EXPECT {
                        trace!("response from {addr} is expected");
                        return duration;
                    }
                    u64::MAX
                },
            )));
        }
        let mut min_dur = u64::MAX;
        let mut min_i = self.index.load(Ordering::SeqCst);
        let ori_i = min_i;
        for (i, task) in tasks.into_iter().enumerate() {
            let dur = task.await.unwrap().unwrap_or_else(|_| {
                warn!("check backend: {} timeout", self.backend_addrs[i]);
                u64::MAX
            });
            self.last_latency[i].store(dur, Ordering::SeqCst);
            if dur < min_dur {
                min_i = i;
                min_dur = dur;
            }
        }

        if min_dur != u64::MAX && min_i != ori_i {
            self.index.store(min_i, Ordering::SeqCst);
            debug!("{} switch to faster backend {min_i}", self.frontend);
        }
    }

    pub fn status(&self) -> String {
        let index = self.index.load(Ordering::SeqCst);
        let active = Arc::clone(&self.backend_addrs[index]);
        let mut res = format!("{{\"active\":\"{}\",\"{}\":[", active, self.frontend);
        for (i, t) in self.back_traffic.iter().enumerate() {
            let addr = Arc::clone(&self.backend_addrs[i]);
            res.push_str(
                format!(
                    "{{\"{}\":{{\"send\":{},\"recv\":{},\"last_latency\":{}}}}},",
                    addr,
                    t.send.load(Ordering::SeqCst),
                    t.recv.load(Ordering::SeqCst),
                    self.last_latency[i].load(Ordering::SeqCst)
                )
                .as_str(),
            );
        }
        res.pop();
        res.push_str("]},");
        res
    }
}

pub struct Proxys(pub Vec<Arc<Proxy>>);

impl Proxys {
    pub fn status(&self) -> String {
        let mut resp = String::from("[");
        for p in self.0.iter() {
            resp.push_str(&p.status());
        }
        resp.pop();
        resp.push(']');
        resp
    }
}

pub async fn process(client: TcpStream, proxy: Arc<Proxy>, uuid: Arc<uuid::Uuid>) {
    let uuid2 = Arc::clone(&uuid);
    let (back_tfc, back_addr) = proxy.fatest_backend();
    let back_tfc2 = Arc::clone(&back_tfc);
    let back_addr2 = Arc::clone(&back_addr);
    let peer_addr = Arc::new(if let Ok(addr) = client.peer_addr() {
        addr.to_string()
    } else {
        String::from("UNKNOWN")
    });
    let peer_addr2 = Arc::clone(&peer_addr);
    let (mut client_r, mut client_w) = tokio::io::split(client);
    let notify_close = Arc::new(tokio::sync::Notify::new());
    let notify_close2 = Arc::clone(&notify_close);

    let back_stream = match TcpStream::connect(back_addr.as_str()).await {
        Ok(s) => s,
        Err(e) => {
            error!("[{uuid}] process failed when build connection with {back_addr}: {e}");
            return;
        }
    };
    let (mut back_r, mut back_w) = tokio::io::split(back_stream);
    debug!("[{uuid}] connected to backend {back_addr}");

    let mut tasks = Vec::with_capacity(2);

    tasks.push(tokio::spawn(async move {
        let mut buf = [0; BUF_LENGTH];
        loop {
            tokio::select!{
                _ = notify_close.notified() => {
                    debug!("[{uuid}] notify backend to close");
                    break;
                }
                r = back_r.read(&mut buf) => {
                    match r {
                        Ok(n) => {
                            if n == 0 {
                                debug!("[{uuid}] [{peer_addr}--{back_addr}] backend has reached EOF");
                                notify_close.notify_one();
                                break;
                            }
                            if let Err(e) = client_w.write_all(&buf[..n]).await {
                                warn!("[{uuid}] [{peer_addr}--{back_addr}] write data(len: {n}) from backend to client error: {e}");
                                notify_close.notify_one();
                                break;
                            }
                            back_tfc2.recv.fetch_add(n as u64, Ordering::SeqCst);
                        }
                        Err(e) => {
                            warn!("[{uuid}] [{peer_addr}--{back_addr}] backend read error: {e}");
                            notify_close.notify_one();
                            break;
                        }
                    }
                }
            }
        }
    }));

    tasks.push(tokio::spawn(async move {
        let mut buf = [0; BUF_LENGTH];
        loop {
                tokio::select!{
                _ = notify_close2.notified() => {
                    debug!("[{uuid2}] notify client to close");
                    break;
                }
                r = client_r.read(&mut buf) => {
                    match r {
                        Ok(n) => {
                            if n == 0 {
                                debug!("[{uuid2}] [{peer_addr2}--{back_addr2}] client has reached EOF");
                                notify_close2.notify_one();
                                break;
                            }
                            if let Err(e) = back_w.write_all(&buf[..n]).await {
                                warn!("[{uuid2}] [{peer_addr2}--{back_addr2}] write data(len: {n}) from client to backend error: {e}");
                                notify_close2.notify_one();
                                break;
                            }
                            back_tfc.send.fetch_add(n as u64, Ordering::SeqCst);
                        }
                        Err(e) => {
                            warn!("[{uuid2}] [{peer_addr2}--{back_addr2}] client read error: {e}");
                            notify_close2.notify_one();
                            break;
                        }
                    }
                }
            }
        }
    }));

    for t in tasks {
        t.await.unwrap();
    }
}
