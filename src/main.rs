use async_smux::{MuxBuilder, MuxConnector};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const HANDSHAKE: &str = "tunrs::handshake::v1::Qt6/oNg5qu+0TX8S+gayngpumyBKy3A+ZXeZV4LP+tE=";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn run_server<A: ToSocketAddrs>(addr: A, route_table: Vec<[String; 2]>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    let (mut stream, peer_addr) = listener.accept().await?;
    println!("[control] connection established from {peer_addr}");

    let mut buf = vec![0u8; HANDSHAKE.len()];

    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .map_err(|_| "handshake timeout")?
        .map_err(|_| "failed to read handshake")?;

    if buf.as_slice() != HANDSHAKE.as_bytes() {
        eprintln!("[control {peer_addr}] invalid handshake");
        return Err("invalid handshake".into());
    }

    println!("[control {peer_addr}] handshake OK");

    let (connector, _acceptor, worker) = MuxBuilder::server().with_connection(stream).build();

    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.await {
            eprintln!("[mux] worker crashed: {e}");
        }
    });

    let mut handles = Vec::with_capacity(route_table.len());

    for [bind_addr, target_addr] in route_table {
        match spawn_route(connector.clone(), &bind_addr, &target_addr).await {
            Ok(handle) => {
                println!("[route {bind_addr} -> {target_addr}] started successfully");
                handles.push(handle);
            }
            Err(e) => {
                eprintln!("[route {bind_addr} -> {target_addr}] failed to start: {e}");
            }
        }
    }

    if handles.is_empty() {
        eprintln!("[server] no routes started");
        return Err("no routes started".into());
    }

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("[route task] crashed: {e}");
        }
    }

    if let Err(e) = worker_handle.await {
        eprintln!("[mux] worker join error: {e}");
    }

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
                    eprintln!("[route {bind} -> {target}] accept error: {e}");
                    continue;
                }
            };

            println!("[route {bind} -> {target}] new connection from {peer_addr}");

            let mut mux = match connector.connect() {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!(
                        "[route {bind} -> {target} | peer {peer_addr}] mux connect error: {e}"
                    );
                    continue;
                }
            };

            let target_bytes = target_bytes.clone();
            let bind = bind.clone();
            let target = target.clone();

            tokio::spawn(async move {
                if let Err(e) = mux.write_all(&target_bytes).await {
                    eprintln!(
                        "[route {bind} -> {target} | peer {peer_addr}] failed to send target: {e}"
                    );
                    return;
                }

                if let Err(e) = copy_bidirectional(&mut inbound, &mut mux).await {
                    let msg = e.to_string();

                    if !msg.contains("closed") {
                        eprintln!("[route {bind} -> {target} | peer {peer_addr}] proxy error: {e}");
                    } else {
                        println!("[route {bind} -> {target} | peer {peer_addr}] connection closed");
                    }
                } else {
                    println!("[route {bind} -> {target} | peer {peer_addr}] transfer completed");
                }
            });
        }
    });

    Ok(jh)
}

async fn open_tunn<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(HANDSHAKE.as_bytes()).await?;
    stream.flush().await?;

    println!("[tunnel] handshake sent");

    let (_connector, mut acceptor, worker) = MuxBuilder::client().with_connection(stream).build();

    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.await {
            eprintln!("[tunnel] mux worker crashed: {e}");
        }
    });

    while let Some(mut stream) = acceptor.accept().await {
        tokio::spawn(async move {
            let n = match stream.read_u8().await {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("[tunnel] failed to read target length: {e}");
                    return;
                }
            };

            let mut buf = vec![0u8; n as usize];

            if let Err(e) = stream.read_exact(&mut buf).await {
                eprintln!("[tunnel] failed to read target addr: {e}");
                return;
            }

            let target = match String::from_utf8(buf) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[tunnel] invalid target utf8: {e}");
                    return;
                }
            };

            println!("[tunnel] new stream -> target {target}");

            let mut local = match TcpStream::connect(&target).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[tunnel -> {target}] connect failed: {e}");
                    return;
                }
            };

            println!("[tunnel -> {target}] connected");

            if let Err(e) = copy_bidirectional(&mut stream, &mut local).await {
                eprintln!("[tunnel -> {target}] transfer error: {e}");
            } else {
                println!("[tunnel -> {target}] closed cleanly");
            }
        });
    }

    if let Err(e) = worker_handle.await {
        eprintln!("[tunnel] mux worker join error: {e}");
    }

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

const VERSION: &str = "tunrs 0.1.1";
const HELP: &str = r#"
tunrs - lightweight tcp tunnel/mux proxy

USAGE:
    tunrs [OPTIONS]

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

                match run_server(rt.tunn.clone(), rt.pairs.clone()).await {
                    Ok(_) => {
                        eprintln!("[server {ta}] exited normally");
                    }
                    Err(e) => {
                        eprintln!("[server {ta}] crashed: {e}");
                    }
                }

                eprintln!("[server {ta}] restarting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
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

                match open_tunn(t.clone()).await {
                    Ok(_) => {
                        eprintln!("[tunnel {ta}] exited normally");
                    }
                    Err(e) => {
                        eprintln!("[tunnel {ta}] crashed: {e}");
                    }
                }

                eprintln!("[tunnel {ta}] restarting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
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
