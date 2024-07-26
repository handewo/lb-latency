use clap::Parser;
use log::{debug, error};
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
    env_logger::builder().init();
    let args = Args::parse();

    let cfg = config::load_config(args.config);

    let mut proxys = Vec::new();
    for pc in cfg.proxys.iter() {
        let mut backend_addrs = Vec::new();
        let mut back_traffic = Vec::new();
        for addr in pc.backend_addrs.iter() {
            backend_addrs.push(Arc::new(addr.clone()));
            back_traffic.push(Arc::new(proxy::Traffic {
                send: AtomicU64::new(0),
                recv: AtomicU64::new(0),
            }));
        }
        proxys.push(Arc::new(proxy::Proxy {
            frontend: pc.frontend,
            index: AtomicUsize::new(0),
            backend_addrs,
            back_traffic,
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
        // Bind the listener to the address
        tokio::spawn(async move {
            let listener = TcpListener::bind(("127.0.0.1", p2.frontend)).await.unwrap();
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