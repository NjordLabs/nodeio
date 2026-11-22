# nodeio

Async graph engine for Python 3.13 services. A graph is validated once
and each call runs on its own values.

```bash
pip install nodeio
```

HTTP and gRPC client nodes are extras: `nodeio[http]` and `nodeio[grpc]`.
Timeouts are required. Plain HTTP and insecure gRPC are off unless the
caller opts in.

The engine depends on
[pymjolnir](https://github.com/NjordLabs/pymjolnir) and
[mimirlog](https://github.com/NjordLabs/mimirlog).
