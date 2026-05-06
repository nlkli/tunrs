# tunrs

```text
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
```
