# lb-latency

**It's just demo for now.**

- It's light simple load balancer.
- Just support low-latency algorithm, it could send http request to all backends, then select the lowest latency one.
- Just support check http protocal not including https.

## build

```bash
# clone this repos first

cd lb-latency
cargo build
```
