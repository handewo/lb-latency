use chrono::Local;
use clap::Parser;
use log::{debug, error, info};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use warp::Filter;

mod config;
mod proxy;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short = 'c', long, default_value_t = String::from("config.toml"))]
    config: String,
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .default_format()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {:<5} {}] {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.module_path().unwrap_or(""),
                record.args()
            )
        })
        .init();
    let args = Args::parse();

    let cfg = config::load_config(args.config);

    let mut proxys = Vec::with_capacity(cfg.proxys.len());
    for pc in cfg.proxys.iter() {
        let len_addrs = pc.backend_addrs.len();
        let mut backend_addrs = Vec::with_capacity(len_addrs);
        let mut back_traffic = Vec::with_capacity(len_addrs);
        let mut last_latency = Vec::with_capacity(len_addrs);
        for addr in pc.backend_addrs.iter() {
            backend_addrs.push(Arc::new(addr.clone()));
            back_traffic.push(Arc::new(proxy::Traffic {
                send: AtomicU64::new(0),
                recv: AtomicU64::new(0),
            }));
            last_latency.push(AtomicU64::new(0))
        }

        proxys.push(Arc::new(proxy::Proxy {
            frontend: pc.frontend.clone(),
            index: AtomicUsize::new(0),
            backend_addrs,
            back_traffic,
            last_latency,
        }));
    }
    let proxys: Arc<proxy::Proxys> = Arc::new(proxy::Proxys(proxys));

    for proxy in proxys.0.iter() {
        let p = Arc::clone(proxy);
        tokio::spawn(async move {
            loop {
                p.check_latency(cfg.check_timeout).await;
                tokio::time::sleep(tokio::time::Duration::from_secs(cfg.check_interval)).await
            }
        });
        let p2 = Arc::clone(proxy);
        let addr = p2.frontend.clone();
        // Bind the listener to the address
        tokio::spawn(async move {
            let listener = try_binding(&addr).await;
            loop {
                // The second item contains the IP and port of the new connection.
                if let Ok((socket, addr)) = listener.accept().await {
                    debug!("accepted a connection from {addr}");
                    let p = Arc::clone(&p2);
                    tokio::spawn(async move {
                        proxy::process(socket, p).await;
                    });
                }
            }
        });
    }
    //let ps = Arc::new(proxy::Proxys(Vec::new()));
    let ps = Arc::clone(&proxys);
    let monitor = warp::path("status").map(move || ps.status());
    warp::serve(monitor)
        .run(cfg.status_addr.parse::<SocketAddr>().unwrap())
        .await;

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
}

async fn try_binding(addr: &str) -> TcpListener {
    loop {
        match TcpListener::bind(addr).await {
            Ok(l) => {
                info!("bind {} successfully", addr);
                return l;
            }
            Err(e) => {
                error!("bind {addr} error: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        };
    }
}
