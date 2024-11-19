use chrono::Local;
use dashmap::DashMap;
use log::{debug, trace, warn};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::vec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration, Instant};

// header "GET / HTTP/1.0\nHOST: 127.0.0.1\n\n"
static REQ_HEADER: &[u8] = b"GET / HTTP/1.0\nHOST: 127.0.0.1\n\n";

//response "HTTP/1.1 200 OK"
static RESP_EXPECT: &[u8] = b"HTTP/1.1 200 OK";

static CLIENT: &str = "client";
static BACKEND: &str = "backend";

const BUF_LENGTH: usize = 65536;

pub struct Proxy {
    pub frontend: String,
    pub index: AtomicUsize,
    pub backend_addrs: Vec<SocketAddr>,
    pub back_traffic: Vec<Traffic>,
    pub last_latency: Vec<AtomicU64>,
    pub conns: DashMap<String, Conn>,
}

pub struct Traffic {
    pub send: AtomicU64,
    pub recv: AtomicU64,
    pub total_requests: AtomicU64,
    pub failed_requests: AtomicU64,
}

#[derive(Hash, PartialEq, Eq)]
pub struct Conn {
    start: String,
    backend: SocketAddr,
}

impl Proxy {
    pub fn fatest_backend(&self) -> (&Traffic, &SocketAddr) {
        let index = self.index.load(Ordering::SeqCst);

        (
            self.back_traffic.get(index).unwrap(),
            self.backend_addrs.get(index).unwrap(),
        )
    }

    pub async fn check_latency(&self, check_timout: u64, check_interval: u64) {
        let backend_addrs: Vec<Arc<SocketAddr>> = self
            .backend_addrs
            .iter()
            .map(|addr| Arc::new(*addr))
            .collect();
        loop {
            let mut tasks = Vec::with_capacity(self.backend_addrs.len());
            for addr in backend_addrs.iter() {
                let addr = addr.clone();
                tasks.push(tokio::spawn(timeout(
                    Duration::from_secs(check_timout),
                    async move {
                        let start = Instant::now();
                        let mut stream = match TcpStream::connect(addr.as_ref()).await {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("check failed when build connection with {addr}: {e}");
                                return u64::MAX;
                            }
                        };
                        stream.write_all(REQ_HEADER).await.unwrap_or_else(|e| {
                            warn!("check falied when send request to {addr}: {e}")
                        });
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
                    warn!("check backend: {} timeout", backend_addrs[i]);
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

            tokio::time::sleep(tokio::time::Duration::from_secs(check_interval)).await;
        }
    }

    pub fn status(&self) -> String {
        let index = self.index.load(Ordering::SeqCst);
        let active = self.backend_addrs.get(index).unwrap();
        let mut res = format!(
            "{{\"active\":\"{}\",\"proxy\":\"{}\",\"backend\":[",
            active, self.frontend
        );
        for (i, t) in self.back_traffic.iter().enumerate() {
            let addr = self.backend_addrs.get(i).unwrap();
            res.push_str(
                format!(
                    "{{\"{}\":{{\"send\":{},\"recv\":{},\"total_requests\":{},\"failed_requests\":{},\"last_latency\":{}}}}},",
                    addr,
                    t.send.load(Ordering::SeqCst),
                    t.recv.load(Ordering::SeqCst),
                    t.total_requests.load(Ordering::SeqCst),
                    t.failed_requests.load(Ordering::SeqCst),
                    self.last_latency[i].load(Ordering::SeqCst)
                )
                .as_str(),
            );
        }
        res.pop();
        res.push_str("],\"current_conn\":[");
        for conn in self.conns.iter() {
            res.push_str(
                format!(
                    "{{\"start\":\"{}\",\"client\":\"{}\",\"backend\":\"{}\"}},",
                    conn.start,
                    conn.key(),
                    conn.backend
                )
                .as_str(),
            );
        }
        match res.pop().unwrap() {
            '[' => res.push_str("[]},"),
            _ => res.push_str("]},"),
        }
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
    let (back_tfc, back_addr) = proxy.fatest_backend();
    let peer_addr = match client.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => String::from("UNKNOWN"),
    };
    let (client_r, client_w) = tokio::io::split(client);
    let conn = Conn {
        start: Local::now().format("%d/%m/%y %H:%M:%S").to_string(),
        backend: *back_addr,
    };

    let back_stream = match TcpStream::connect(back_addr).await {
        Ok(s) => {
            back_tfc.total_requests.fetch_add(1, Ordering::SeqCst);
            proxy.conns.insert(peer_addr.clone(), conn);
            s
        }
        Err(e) => {
            warn!("[{uuid}] process failed when build connection with {back_addr}: {e}");
            back_tfc.failed_requests.fetch_add(1, Ordering::SeqCst);
            return;
        }
    };
    debug!("[{uuid}] connected to backend {back_addr}");
    let (back_r, back_w) = tokio::io::split(back_stream);

    let _ = tokio::try_join!(
        read_write(&uuid, back_r, client_w, &peer_addr, back_addr, back_tfc, BACKEND),
        read_write(&uuid, client_r, back_w, &peer_addr, back_addr, back_tfc, CLIENT)
    )
    .is_ok();
    proxy.conns.remove(&peer_addr);
}

async fn read_write<T>(
    uuid: &uuid::Uuid,
    mut from: ReadHalf<T>,
    mut to: WriteHalf<T>,
    from_addr: &str,
    to_addr: &SocketAddr,
    traffic: &Traffic,
    dir: &'static str,
) -> Result<(), ()>
where
    T: AsyncRead + AsyncWrite,
{
    let mut buf = [0; BUF_LENGTH];
    let counter = if dir == CLIENT {
        &traffic.send
    } else {
        &traffic.recv
    };
    loop {
        match from.read(&mut buf).await {
            Ok(n) => {
                if n == 0 {
                    debug!("[{uuid}] [{from_addr}--{to_addr}] {dir} has reached EOF");
                    return Err(());
                }
                if let Err(e) = to.write_all(&buf[..n]).await {
                    traffic.failed_requests.fetch_add(1, Ordering::SeqCst);
                    warn!("[{uuid}] [{from_addr}--{to_addr}] write data(len: {n}) from {dir} error: {e}");
                    return Err(());
                }
                counter.fetch_add(n as u64, Ordering::SeqCst);
            }
            Err(e) => {
                traffic.failed_requests.fetch_add(1, Ordering::SeqCst);
                warn!("[{uuid}] [{from_addr}--{to_addr}] {dir} read error: {e}");
                return Err(());
            }
        }
    }
}
