use async_smux::{MuxBuilder, MuxConnector};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const HANDSHAKE: &str = "tunrs::handshake::v1";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn run_server<A: ToSocketAddrs>(addr: A, route_table: Vec<[SocketAddr; 2]>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    let (mut stream, peer_addr) = listener.accept().await?;
    println!("control connection established: {peer_addr}");

    let mut buf = vec![0u8; HANDSHAKE.len()];

    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .map_err(|_| "handshake timeout")?
        .map_err(|_| "failed to read handshake")?;

    if buf.as_slice() != HANDSHAKE.as_bytes() {
        return Err("invalid handshake".into());
    }

    let (connector, _acceptor, worker) = MuxBuilder::server().with_connection(stream).build();

    let worker_handle = tokio::spawn(worker);

    let mut handles = Vec::with_capacity(route_table.len());
    for [bind_addr, target_addr] in route_table {
        match spawn_route(connector.clone(), bind_addr, target_addr).await {
            Ok(handle) => {
                println!("route started: {bind_addr} -> {target_addr}");
                handles.push(handle);
            }
            Err(e) => {
                eprintln!("failed to start route {bind_addr} -> {target_addr}: {e}");
            }
        }
    }

    if handles.is_empty() {
        return Err("no routes started".into());
    }

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("route task crashed: {e}");
        }
    }

    if let Err(e) = worker_handle.await {
        eprintln!("mux worker crashed: {e}");
    }

    Ok(())
}

async fn spawn_route(
    connector: MuxConnector<TcpStream>,
    bind_addr: SocketAddr,
    target_addr: SocketAddr,
) -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr).await?;

    let mut target_bytes = Vec::with_capacity(18);
    match target_addr.ip() {
        IpAddr::V4(ip) => target_bytes.extend_from_slice(&ip.octets()),
        IpAddr::V6(ip) => target_bytes.extend_from_slice(&ip.octets()),
    }
    target_bytes.extend_from_slice(&target_addr.port().to_be_bytes());

    let jh = tokio::spawn(async move {
        loop {
            let (mut inbound, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    continue;
                }
            };

            let mut mux = match connector.connect() {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!("connect error ({peer_addr}): {e}");
                    continue;
                }
            };

            let target_bytes = target_bytes.clone();

            tokio::spawn(async move {
                if let Err(e) = mux.write_all(&target_bytes).await {
                    eprintln!("write target addr failed ({peer_addr}): {e}");
                    return;
                }

                if let Err(e) = copy_bidirectional(&mut inbound, &mut mux).await {
                    let msg = e.to_string();

                    if !msg.contains("closed") {
                        eprintln!("proxy error ({peer_addr}): {e}");
                    }
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

    let (_connector, mut acceptor, worker) = MuxBuilder::client().with_connection(stream).build();

    let worker_handle = tokio::spawn(worker);

    while let Some(mut stream) = acceptor.accept().await {
        tokio::spawn(async move {
            let mut ip_buf = [0u8; 16];
            let ip = match stream.read_exact(&mut ip_buf[..4]).await {
                Ok(_) => {
                    std::net::IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&ip_buf[..4]).unwrap()))
                }
                Err(e) => {
                    eprintln!("read ip failed: {e}");
                    return;
                }
            };

            let port = match stream.read_u16().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("read port failed: {e}");
                    return;
                }
            };

            let target = SocketAddr::new(ip, port);
            eprintln!("connect target: {target}");

            let mut local = match TcpStream::connect(target).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("connect target failed ({target}): {e}");
                    return;
                }
            };

            if let Err(e) = copy_bidirectional(&mut stream, &mut local).await {
                eprintln!("tunnel error ({target}): {e}");
            }
        });
    }

    if let Err(e) = worker_handle.await {
        eprintln!("mux worker crashed: {e}");
    }

    Ok(())
}

#[derive(Debug, Default)]
pub struct Args {
    tunn: String,
    port: Option<u16>,
    route_table: Vec<[SocketAddr; 2]>,
}

const VERSION: &str = "tunrs 0.1.0";
const HELP: &str = r#"
tunrs - lightweight tcp tunnel/mux proxy

USAGE:
    tunn [OPTIONS]

OPTIONS:
    -t, --tunn <ADDR>          remote tunnel address (client mode)
    -p, --port <PORT>          local server port (default: 8080)
    -r, --route <A> <B>        route mapping (can be repeated)
    --help                     show this help
    --version                  show version

EXAMPLES:
    tunrs --tunn 1.2.3.4:9000

    tunrs --port 8080 \
         --route 127.0.0.1:3000 10.0.0.1:80 \
         --route 127.0.0.1:4000 10.0.0.2:443

MODES:
    - server mode: when --route is provided
    - tunnel mode: when --tunn is provided
"#;

impl Args {
    pub fn parse() -> Self {
        let mut args = std::env::args().skip(1);

        let mut res = Self::default();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--tunn" | "-t" => {
                    res.tunn = args.next().unwrap_or_else(|| {
                        eprintln!("--tunn requires value");
                        std::process::exit(1);
                    });
                }

                "--port" | "-p" => {
                    let v = args.next().unwrap_or_else(|| {
                        eprintln!("--port requires value");
                        std::process::exit(1);
                    });

                    res.port = Some(v.parse().unwrap_or_else(|_| {
                        eprintln!("invalid port: {v}");
                        std::process::exit(1);
                    }));
                }

                "--route" | "-r" => {
                    let a = args.next().unwrap_or_else(|| {
                        eprintln!("--route requires 2 addresses");
                        std::process::exit(1);
                    });

                    let b = args.next().unwrap_or_else(|| {
                        eprintln!("--route requires 2 addresses");
                        std::process::exit(1);
                    });

                    let a: SocketAddr = a.parse().unwrap_or_else(|_| {
                        eprintln!("invalid addr: {a}");
                        std::process::exit(1);
                    });

                    let b: SocketAddr = b.parse().unwrap_or_else(|_| {
                        eprintln!("invalid addr: {b}");
                        std::process::exit(1);
                    });

                    res.route_table.push([a, b]);
                }

                "--help" => {
                    println!("{HELP}");
                    std::process::exit(0);
                }

                "--version" | "-V" => {
                    println!("{VERSION}");
                    std::process::exit(0);
                }

                _ => {
                    eprintln!("unknown argument: {arg}");
                    std::process::exit(1);
                }
            }
        }

        res
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut attempt = 0;

    if !args.route_table.is_empty() {
        let port = args.port.unwrap_or(8080);
        let bind_addr = format!("0.0.0.0:{port}");

        eprintln!("[server] starting on {bind_addr}");

        loop {
            attempt += 1;

            eprintln!("[server] run attempt #{attempt}");

            if let Err(e) = run_server(bind_addr.clone(), args.route_table.clone()).await {
                eprintln!("[server] crashed (attempt #{attempt}): {e}");
            }

            eprintln!("[server] restarting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    if !args.tunn.is_empty() {
        eprintln!("[tunnel] connecting to {}", args.tunn);

        loop {
            attempt += 1;

            eprintln!("[tunnel] run attempt #{attempt}");

            if let Err(e) = open_tunn(args.tunn.clone()).await {
                eprintln!("[tunnel] crashed (attempt #{attempt}): {e}");
            }

            eprintln!("[tunnel] restarting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(())
}
