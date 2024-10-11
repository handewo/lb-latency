use log::{debug, error, trace, warn};
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
    pub backend_addrs: Vec<Arc<String>>,
    pub back_traffic: Vec<Arc<Traffic>>,
    pub last_latency: Vec<AtomicU64>,
}

pub struct Traffic {
    pub send: AtomicU64,
    pub recv: AtomicU64,
    pub total_requests: AtomicU64,
    pub failed_requests: AtomicU64,
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
    let (client_r, client_w) = tokio::io::split(client);

    let back_stream = match TcpStream::connect(back_addr.as_str()).await {
        Ok(s) => {
            back_tfc.total_requests.fetch_add(1, Ordering::SeqCst);
            s
        }
        Err(e) => {
            error!("[{uuid}] process failed when build connection with {back_addr}: {e}");
            back_tfc.failed_requests.fetch_add(1, Ordering::SeqCst);
            return;
        }
    };
    debug!("[{uuid}] connected to backend {back_addr}");
    let (back_r, back_w) = tokio::io::split(back_stream);

    let _ = tokio::try_join!(
        read_write(uuid, back_r, client_w, peer_addr, back_addr, back_tfc, BACKEND),
        read_write(uuid2, client_r, back_w, peer_addr2, back_addr2, back_tfc2, CLIENT)
    )
    .is_ok();
}

async fn read_write<T>(
    uuid: Arc<uuid::Uuid>,
    mut from: ReadHalf<T>,
    mut to: WriteHalf<T>,
    from_addr: Arc<String>,
    to_addr: Arc<String>,
    traffic: Arc<Traffic>,
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
