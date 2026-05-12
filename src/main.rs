use async_smux::{MuxBuilder, MuxConnector};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const HANDSHAKE: &str = "tunrs::handshake::v1::Qt6/oNg5qu+0TX8S+gayngpumyBKy3A+ZXeZV4LP+tE=";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(9);
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(6);

const CLIENT_KEEP_ALIVE_INTERVAL: std::num::NonZeroU64 = std::num::NonZeroU64::new(18).unwrap();

const SERVER_IDLE_TIMEOUT: std::num::NonZeroU64 = std::num::NonZeroU64::new(60).unwrap();

async fn run_server(addr: &str, route_table: Vec<[String; 2]>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    println!("[server {addr}] listening for control connections");

    let (stream, _peer_addr) = loop {
        let (mut stream, peer_addr) = listener.accept().await?;

        println!("[server {addr}] incoming connection from {peer_addr}");

        let mut buf = vec![0u8; HANDSHAKE.len()];
        if tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf))
            .await
            .is_err()
        {
            eprintln!(
                "[server {addr}] <- {peer_addr}: handshake timed out after {:?}",
                HANDSHAKE_TIMEOUT
            );

            continue;
        }

        if buf.as_slice() != HANDSHAKE.as_bytes() {
            eprintln!("[server {addr}] invalid handshake received: {peer_addr}");
            continue;
        }

        println!("[server {addr}] handshake accepted: {peer_addr}");

        break (stream, peer_addr);
    };

    let (connector, _acceptor, worker) = MuxBuilder::server()
        .with_idle_timeout(SERVER_IDLE_TIMEOUT)
        .with_connection(stream)
        .build();

    let worker_handle = tokio::spawn({
        let addr = addr.to_string();
        async move {
            if let Err(e) = worker.await {
                eprintln!("[server {addr}] mux worker terminated with error: {e}");
            } else {
                println!("[server {addr}] mux worker stopped");
            }
        }
    });

    let mut handles = Vec::with_capacity(route_table.len());

    for [bind_addr, target_addr] in route_table {
        match spawn_route(connector.clone(), &bind_addr, &target_addr).await {
            Ok(handle) => {
                println!("[server {addr}] {bind_addr} -> {target_addr} route initialized");

                handles.push(handle);
            }
            Err(e) => {
                eprintln!(
                    "[server {addr}] {bind_addr} -> {target_addr} route initialization failed: {e}"
                );
            }
        }
    }

    if handles.is_empty() {
        eprintln!("[server {addr}] no active routes");

        return Err("no routes started".into());
    }

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("[server {addr}] route task join failed: {e}");
        }
    }

    if let Err(e) = worker_handle.await {
        eprintln!("[server {addr}] worker join failed: {e}");
    }

    println!("[server {addr}] shutdown complete");

    Ok(())
}

async fn spawn_route(
    connector: MuxConnector<TcpStream>,
    bind_addr: &str,
    target_addr: &str,
) -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr).await?;

    let mut target_bytes = Vec::with_capacity(target_addr.len() + 1);

    target_bytes.push(target_addr.len() as u8);
    target_bytes.extend_from_slice(target_addr.as_bytes());

    let bind = bind_addr.to_string();
    let target = target_addr.to_string();

    let jh = tokio::spawn(async move {
        println!("[route {bind} -> {target}] listening");

        loop {
            let (mut inbound, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[route {bind} -> {target}] accept failed: {e}");

                    continue;
                }
            };

            println!("[route {bind} -> {target}] accepted connection from {peer_addr}");

            let mut mux = match connector.connect() {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!(
                        "[route {bind} -> {target} | {peer_addr}] mux stream creation failed: {e}"
                    );

                    continue;
                }
            };

            let target_bytes = target_bytes.clone();
            let bind = bind.clone();
            let target = target.clone();

            tokio::spawn(async move {
                let peer_addr_str = peer_addr.to_string();

                let mut peer_bytes = Vec::with_capacity(peer_addr_str.len() + 1);

                peer_bytes.push(peer_addr_str.len() as u8);
                peer_bytes.extend_from_slice(peer_addr_str.as_bytes());

                if let Err(e) = mux.write_all(&peer_bytes).await {
                    eprintln!(
                        "[route {bind} -> {target} | {peer_addr}] failed to send peer metadata: {e}"
                    );

                    return;
                }

                if let Err(e) = mux.write_all(&target_bytes).await {
                    eprintln!(
                        "[route {bind} -> {target} | {peer_addr}] failed to send target metadata: {e}"
                    );

                    return;
                }

                if let Err(e) = mux.flush().await {
                    eprintln!(
                        "[route {bind} -> {target} | {peer_addr}] metadata flush failed: {e}"
                    );

                    return;
                }

                println!("[route {bind} -> {target} | {peer_addr}] forwarding started");

                match copy_bidirectional(&mut inbound, &mut mux).await {
                    Ok((from_client, from_target)) => {
                        println!(
                            "[route {bind} -> {target} | {peer_addr}] forwarding completed (client={}B, target={}B)",
                            from_client, from_target
                        );
                    }
                    Err(e) => {
                        let msg = e.to_string();

                        if msg.contains("closed") {
                            println!("[route {bind} -> {target} | {peer_addr}] connection closed");
                        } else {
                            eprintln!(
                                "[route {bind} -> {target} | {peer_addr}] forwarding error: {e}"
                            );
                        }
                    }
                }
            });
        }
    });

    Ok(jh)
}

async fn open_tunn(addr: &str) -> Result<()> {
    println!("[tunnel {addr}] connecting");

    let mut stream = TcpStream::connect(addr).await?;

    stream.write_all(HANDSHAKE.as_bytes()).await?;
    stream.flush().await?;

    println!("[tunnel {addr}] handshake sent");

    let (_connector, mut acceptor, worker) = MuxBuilder::client()
        .with_keep_alive_interval(CLIENT_KEEP_ALIVE_INTERVAL)
        .with_connection(stream)
        .build();

    let worker_handle = tokio::spawn({
        let addr = addr.to_string();

        async move {
            if let Err(e) = worker.await {
                eprintln!("[tunnel {addr}] mux worker terminated: {e}");
            } else {
                println!("[tunnel {addr}] mux worker stopped");
            }
        }
    });

    println!("[tunnel {addr}] ready");

    while let Some(mut stream) = acceptor.accept().await {
        tokio::spawn({
            let addr = addr.to_string();

            async move {
                let n = match stream.read_u8().await {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("[tunnel {addr}] failed to read peer address length: {e}");

                        return;
                    }
                };

                let mut buf = vec![0u8; n as usize];
                if let Err(e) = stream.read_exact(&mut buf).await {
                    eprintln!("[tunnel {addr}] failed to read peer address: {e}");

                    return;
                }

                let peer_addr = match String::from_utf8(buf) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[tunnel {addr}] invalid peer address encoding: {e}");

                        return;
                    }
                };

                let n = match stream.read_u8().await {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("[tunnel {addr}] failed to read target length: {e}");

                        return;
                    }
                };

                let mut buf = vec![0u8; n as usize];
                if let Err(e) = stream.read_exact(&mut buf).await {
                    eprintln!("[tunnel {addr}] failed to read target address: {e}");

                    return;
                }

                let target = match String::from_utf8(buf) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[tunnel {addr}] invalid target address encoding: {e}");

                        return;
                    }
                };

                println!("[tunnel {addr}] incoming stream: {peer_addr} -> {target}");

                let mut local = match TcpStream::connect(&target).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!(
                            "[tunnel {addr}] target connection failed: {peer_addr} -> {target}: {e}"
                        );

                        return;
                    }
                };

                println!("[tunnel {addr}] target connected: {peer_addr} -> {target}");

                match copy_bidirectional(&mut stream, &mut local).await {
                    Ok((from_remote, from_local)) => {
                        println!(
                            "[tunnel {addr}] stream closed: {peer_addr} -> {target} (remote={}B, local={}B)",
                            from_remote, from_local
                        );
                    }
                    Err(e) => {
                        eprintln!("[tunnel {addr}] transfer failed: {peer_addr} -> {target}: {e}");
                    }
                }
            }
        });
    }

    if let Err(e) = worker_handle.await {
        eprintln!("[tunnel {addr}] worker join failed: {e}");
    }

    println!("[tunnel {addr}] shutdown complete");

    Ok(())
}

#[inline(always)]
fn parse_addr(s: String) -> Result<String> {
    if let Ok(p) = s.parse::<u16>() {
        return Ok(format!("0.0.0.0:{p}"));
    }

    Ok(s.parse::<SocketAddr>()?.to_string())
}

// #[derive(Debug, Default)]
// struct Config {
//     tunn: Vec<String>,
//     route_tables: Vec<RouteTable>,
// }
//
// impl Config {
//     fn parse_file(path: &str) -> Result<Config> {
//         todo!()
//     }
// }

#[derive(Debug, Default)]
struct RouteTable {
    tunn: String,
    pairs: Vec<[String; 2]>,
}

#[derive(Debug, Default)]
struct Args {
    tunn: Vec<String>,
    route_table: Vec<RouteTable>,
}

const VERSION: &str = "tunrs 0.1.2 [https://github.com/nlkli/tunrs]";
const HELP: &str = r#"
tunrs - lightweight tcp tunnel/mux proxy

https://github.com/nlkli/tunrs

OPTIONS:
    -t, --tunn <ADDR>...
        remote tunnel address (client mode, repeatable)

    -r, --route <TUNN> <A> <B> [<A> <B> ...]
        route table: tunnel + one or more address pairs

    -h, --help
    -V, --version

EXAMPLES:
    # client mode
    tunrs --tunn 1.2.3.4:9000

    # server mode
    tunrs \
        --route 0.0.0.0:9000 \
            127.0.0.1:3000 10.0.0.1:80 \
            127.0.0.1:4000 10.0.0.2:443
"#;

impl Args {
    pub fn parse() -> Self {
        let mut args = std::env::args().skip(1).peekable();
        let mut res = Self::default();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--tunn" | "-t" => {
                    let raw = args.next().unwrap_or_else(|| {
                        eprintln!("error: --tunn requires <ADDR>");
                        std::process::exit(1);
                    });

                    let addr = parse_addr(raw).unwrap_or_else(|e| {
                        eprintln!("error: invalid address: {e}");
                        std::process::exit(1);
                    });

                    res.tunn.push(addr);
                }

                "--route" | "-r" => {
                    let mut rt = RouteTable::default();

                    let raw_tunn = args.next().unwrap_or_else(|| {
                        eprintln!("error: --route requires <TUNN> <A> <B> [...]");
                        std::process::exit(1);
                    });

                    rt.tunn = parse_addr(raw_tunn).unwrap_or_else(|e| {
                        eprintln!("error: invalid address: {e}");
                        std::process::exit(1);
                    });

                    while let Some(peek) = args.peek() {
                        if peek.starts_with('-') {
                            break;
                        }

                        let a = args.next().unwrap();
                        let b = args.next().unwrap_or_else(|| {
                            eprintln!("error: --route requires pairs <A> <B>");
                            std::process::exit(1);
                        });

                        let a = parse_addr(a).unwrap_or_else(|e| {
                            eprintln!("error: invalid address: {e}");
                            std::process::exit(1);
                        });

                        let b = parse_addr(b).unwrap_or_else(|e| {
                            eprintln!("error: invalid address: {e}");
                            std::process::exit(1);
                        });

                        rt.pairs.push([a, b]);
                    }

                    if rt.pairs.is_empty() {
                        eprintln!("error: --route requires at least one pair <A> <B>");
                        std::process::exit(1);
                    }

                    res.route_table.push(rt);
                }

                "--help" | "-h" => {
                    println!("{HELP}");
                    std::process::exit(0);
                }

                "--version" | "-V" => {
                    println!("{VERSION}");
                    std::process::exit(0);
                }

                _ => {
                    eprintln!("error: unknown argument '{arg}'");
                    std::process::exit(1);
                }
            }
        }

        if res.tunn.is_empty() && res.route_table.is_empty() {
            eprintln!("error: specify --tunn or --route");
            std::process::exit(1);
        }

        res
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut handles = Vec::new();

    for rt in args.route_table {
        let ta = rt.tunn.clone();

        println!("[server] starting on {ta}");

        handles.push(tokio::spawn(async move {
            let mut attempt = 0;

            loop {
                attempt += 1;
                eprintln!("[server {ta}] attempt #{attempt}");

                match run_server(&rt.tunn, rt.pairs.clone()).await {
                    Ok(_) => {
                        eprintln!("[server {ta}] exited normally");
                    }
                    Err(e) => {
                        eprintln!("[server {ta}] crashed: {e}");
                    }
                }

                eprintln!(
                    "[server {ta}] restarting in {}s...",
                    RECONNECT_TIMEOUT.as_secs()
                );
                tokio::time::sleep(RECONNECT_TIMEOUT).await;
            }
        }));
    }

    for t in args.tunn {
        let ta = t.clone();

        println!("[tunnel] connecting to {ta}");

        handles.push(tokio::spawn(async move {
            let mut attempt = 0;

            loop {
                attempt += 1;
                eprintln!("[tunnel {ta}] attempt #{attempt}");

                match open_tunn(&t).await {
                    Ok(_) => {
                        eprintln!("[tunnel {ta}] exited normally");
                    }
                    Err(e) => {
                        eprintln!("[tunnel {ta}] crashed: {e}");
                    }
                }

                eprintln!(
                    "[tunnel {ta}] restarting in {}s...",
                    RECONNECT_TIMEOUT.as_secs()
                );
                tokio::time::sleep(RECONNECT_TIMEOUT).await;
            }
        }));
    }

    tokio::signal::ctrl_c().await?;
    eprintln!("shutdown signal received");

    for h in handles {
        h.abort();
    }

    Ok(())
}
