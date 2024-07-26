# lb-latency

**It's just demo for now.**

- It's light simple load balancer.
- Just support low-latency algorithm, it could send http request to all backends, then select the lowest latency one.
- Just support check http protocal not including https.

## build

```bash
# clone this repos first
git clone https://github.com/handewo/lb-latency

cd lb-latency
cargo run

# get status
curl http://127.0.0.1:3030/status
```

```json
[
  {
    "active": "192.168.1.1:80",
    "127.0.0.1:5555": [
      {
        "192.168.1.1:80": {
          "send": 0,
          "recv": 0,
          "last_latency": 0
        }
      },
      {
        "192.168.1.2:80": {
          "send": 0,
          "recv": 0,
          "last_latency": 0
        }
      }
    ]
  },
  {
    "active": "192.168.2.1:80",
    "127.0.0.1:3333": [
      {
        "192.168.2.1:80": {
          "send": 0,
          "recv": 0,
          "last_latency": 0
        }
      },
      {
        "192.168.2.2:80": {
          "send": 0,
          "recv": 0,
          "last_latency": 0
        }
      }
    ]
  }
]
```
