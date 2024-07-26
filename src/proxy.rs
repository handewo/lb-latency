use log::{debug, error, trace, warn};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    pub frontend: String,
    pub index: AtomicUsize,
    pub backend_addrs: Vec<Arc<String>>,
    pub back_traffic: Vec<Arc<Traffic>>,
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
                            error!("check failed when build connection with {addr}: {e}");
                            return u64::MAX;
                        }
                    };
                    stream
                        .write_all(REQ_HEADER)
                        .await
                        .unwrap_or_else(|e| warn!("check falied when send request to {addr}: {e}"));
                    let mut buf = vec![0; 256];
                    if let Err(e) = stream.read(&mut buf).await {
                        error!("read error when check {addr}: {e}");
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

    pub fn status(&self) -> String {
        let index = self.index.load(Ordering::SeqCst);
        let active = Arc::clone(&self.backend_addrs[index]);
        let mut res = format!("{{\"active\":\"{}\",\"{}\":[", active, self.frontend);
        for (i, t) in self.back_traffic.iter().enumerate() {
            let addr = Arc::clone(&self.backend_addrs[i]);
            res.push_str(
                format!(
                    "{{\"{}\":{{\"send\":{},\"recv\":{}}}}},",
                    addr,
                    t.send.load(Ordering::SeqCst),
                    t.recv.load(Ordering::SeqCst)
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
pub async fn process(mut socket: TcpStream, proxy: Arc<Proxy>) {
    let mut s_buf = vec![0; 1024];
    let mut b_buf = vec![0; 1024];
    let (back_tfc, back_addr) = proxy.fatest_backend();

    let mut back_stream = match TcpStream::connect(back_addr.as_str()).await {
        Ok(s) => s,
        Err(e) => {
            error!("process failed when build connection with {back_addr}: {e}");
            return;
        }
    };
    trace!("connected to backend {back_addr}");

    loop {
        tokio::select! {
            Ok(n) = socket.read(&mut s_buf) => {
                if n == 0 {
                    trace!("client has reached EOF");
                    break;
                }
                trace!("read data from client");
                if let Err(e) = back_stream.write(&s_buf[..n]).await {
                    warn!("write data from client to backend error: {e}");
                    break;
                }
            back_tfc.send.fetch_add(n as u64 ,Ordering::SeqCst);
            }
            Ok(n) = back_stream.read(&mut b_buf) => {
                if n == 0 {
                    trace!("backend has reached EOF");
                    break;
                }
                trace!("read data from backend");
                if let Err(e) = socket.write(&b_buf[..n]).await {
                    warn!("write data from back_end to client error: {e}");
                    break;
                }
            back_tfc.recv.fetch_add(n as u64,Ordering::SeqCst);
            }
            else => {
                warn!("client or backend read error");
                break
            }
        }
    }
}
