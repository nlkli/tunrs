
#[derive(Debug, Default)]
pub struct RouteTable {
    pub tunn: String,
    pub pairs: Vec<[String; 2]>,
}

#[derive(Debug, Default)]
pub struct Args {
    pub tunn: Vec<String>,
    pub route_table: Vec<RouteTable>,
}

pub const VERSION: &str = "tunrs 0.1.2 [https://github.com/nlkli/tunrs]";
pub const HELP: &str = r#"
tunrs - lightweight tcp tunnel/mux proxy

https://github.com/nlkli/tunrs

OPTIONS:
    -t, --tunn <ADDR>...
        remote tunnel address (client mode, repeatable)

    -r, --route <TUNN> <A> <B> [<A> <B> ...]
        route table: tunnel + one or more address pairs
        incoming conn -> <A> -> <TUNN> -> <B>

    -h, --help
    -V, --version

EXAMPLES:
    # client mode
    tunrs --tunn 1.2.3.4:9000

    # server mode
    tunrs \
        --route 0.0.0.0:9000 \
            3000           10.0.0.1:80 \
            127.0.0.1:4000 22
"#;


#[inline(always)]
fn parse_addr(s: String) -> Result<String, String> {
    if let Ok(p) = s.parse::<u16>() {
        return Ok(format!("0.0.0.0:{p}"));
    }

    // Ok(s.parse::<SocketAddr>()?.to_string())
    Ok(s)
}


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

