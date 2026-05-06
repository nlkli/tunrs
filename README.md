# tunrs

```text
tunrs - lightweight tcp tunnel/mux proxy

USAGE:
    tunrs [OPTIONS]

OPTIONS:
    -t, --tunn <ADDR>...
        remote tunnel address (client mode, repeatable)

    -r, --route <TUNN> <A> <B> [<A> <B> ...]
        route table: tunnel + one or more address pairs

    -h, --help
        show this help

    -V, --version
        show version

EXAMPLES:
    # client mode
    tunrs --tunn 1.2.3.4:9000

    # server mode
    tunrs \
        --route 0.0.0.0:9000 \
            127.0.0.1:3000 10.0.0.1:80 \
            127.0.0.1:4000 10.0.0.2:443
```
