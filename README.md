# 🐉 HydraStream

[![PyPI version](https://badge.fury.io/py/hydrastream.svg)](https://pypi.org/project/hydrastream/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml)

<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="HydraStream Demo" width="800">
</p>

A high-performance, fault-tolerant, and streaming-capable downloader for Big Data. Built with pure Python, `uvloop`, and `httpx`.

---

## 💡 The Problem vs. The Solution

**The Problem:** Downloading massive datasets (ML weights, DB dumps, genomic sequences) using standard tools like `wget` or `curl` is slow due to single-connection limits. Furthermore, processing these huge files usually requires saving them to disk first, creating severe I/O bottlenecks and requiring massive storage.

**The Solution:** `HydraStream` acts like a multi-headed beast. It utilizes HTTP/2 multiplexing and concurrent chunk downloading to max out your bandwidth. **Its killer feature is the Sequential Reordering Buffer**, which instantly converts chaotic, multi-threaded downloads into a strict sequential byte stream, allowing you to pipe terabytes of data directly into other tools without ever touching your hard drive.

---

## ✨ Key Features
* 🚀 **Maximized Throughput:** Concurrent chunk downloading using `uvloop`.
* 🌊 **True In-Memory Streaming:** Downloads chunks asynchronously but yields them sequentially. Pipe data directly into parsers or Unix tools (zero disk I/O).
* 🛡️ **Bulletproof Reliability (The Hydra):**
  * **AIMD Rate Limiting & Circuit Breaker** to prevent IP bans.
  * **Exponential Backoff + Full Jitter** for network drops.
  * **Partial Chunk Commits:** If a connection drops, it saves the exact byte offset. You never lose progress.
* 🎯 **The "Unplug" Challenge:** *We dare you to break it.* Start a massive download, disable your Wi-Fi, close your laptop lid, or send a `SIGSTOP` to the process. Wait 10 minutes. Turn it back on. HydraStream won't crash. It will patiently wait, auto-recover, and resume from the exact byte where it left off.
* 🧩 **Smart Integrity Validation:** Automatically extracts and verifies MD5 checksums from AWS S3 (`ETag`), Google Cloud (`x-goog-hash`), standard HTTP headers, and NCBI provider files.
* 💾 **Atomic Writes:** Uses low-level `os.pwrite` to prevent Global Interpreter Lock (GIL) bottlenecks during disk I/O.
* 📊 **Adaptive UI:**
  * **Default:** Beautiful, dynamic terminal UI powered by `Rich` (gradients, global ETA).
  * `-nu / --no-ui`: Plain text logs for CI/CD environments.
  * `-q / --quiet`: Strict POSIX compliance (stderr for logs, stdout for data streams).

---

## 📊 Performance & Benchmarks

**Throughput (Datacenter Environment):**
When running on gigabit backbone networks (e.g., AWS, GitHub Actions), `HydraStream` matches the raw I/O performance of C++ utilities like `aria2`. Both saturate the server-side limits identically, proving that the Python/uvloop implementation adds **zero overhead**.

**The Real-World Advantage (High Latency Networks):**
Standard tools like `wget` or `curl` use a single TCP connection. On residential or corporate networks with high latency (e.g., 100ms+ ping to NCBI servers), single-stream TCP is physically bottlenecked by the TCP Window Size (Bandwidth-Delay Product), often capping at 2-5 MB/s.
`HydraStream` bypasses this by multiplexing the file across 20+ connections, fully saturating your local ISP bandwidth.

**The Killer Feature (Streaming):**
Unlike `aria2`, which must write to disk to reconstruct multi-part downloads, `HydraStream` uses an in-memory `heapq` to reorder chunks on the fly. This allows you to achieve multi-connection speeds **while piping data directly into other Unix tools**, saving terabytes of SSD wear-and-tear.

---

## 🛠 Installation

Requires Python 3.11+. The easiest way to use HydraStream as a CLI app is to install it globally via `uv` (recommended) or `pipx`:

```bash
uv tool install hydrastream
```
```bash
pipx install hydrastream
```
To use it as a Python library in your own projects:
```bash
uv add hydrastream
```
```bash
pip install hydrastream
```

You can use hydrastream, hstream, or simply hs to run the tool from anywhere in your system:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```

---

## 🚀 Usage

### 1. Basic Download (Disk Mode)
Download a file using 20 concurrent connections:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="Disk Download Demo" width="800">
</p>
*(If interrupted, rerun the exact command to resume from the last saved byte).*

### 2. Unix Pipeline Streaming (The Killer Feature) 💥
Download a compressed 100GB file, decompress it in memory, and process it—**without saving the archive to your disk**:

```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream --quiet | zcat | grep -c "^>"
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Pipeline-Streaming-Demo.gif" alt="Pipeline Streaming Demo" width="800">
</p>

### 3. Use as a Python Library (For Data Science / MLOps)
Embed the streaming engine directly into your PyTorch/Pandas data loaders:

```python
import asyncio
from hydrastream import HydraStream

async def main():
    urls =["https://url1.gz", "https://url2.gz"]

    async with HydraStream(threads=10, quiet=True) as loader:
        async for filename, stream in loader.stream_all(urls):
            print(f"Processing {filename}...")
            async for chunk_bytes in stream:
                # Feed raw bytes to your parser, ML model, or decompressor
                process_data(chunk_bytes)

asyncio.run(main())
```

---

## ⚙️ CLI Options

| Option | Shortcut | Default | Description |
| :--- | :---: | :---: | :--- |
| `URLS` | - | Required | One or multiple URLs to download (separated by space). |
| `--threads` | `-t` | `1` | Number of concurrent connections. |
| `--output` | `-o` | `download/` | Directory to save files and `.state.json` trackers. |
| `--stream` | `-s` | `False` | Enable streaming mode (redirects data to `stdout`). |
| `--no-ui` | `-nu` | `False` | Disables progress bars, leaves plain text logs. |
| `--quiet` | `-q` | `False` | Dead silence. No console output at all (for strict pipelines). |
| `--md5` | | `None` | Expected MD5 hash (works only if a single URL is provided). |
| `--buffer` | `-b` | `threads * 5MB`| Maximum stream buffer size in bytes. |
---

## 🧠 Under the Hood (Architecture)

For those interested in System Design, this tool implements several advanced engineering patterns:

* **Network Congestion Control (AIMD):** Implements an Additive Increase / Multiplicative Decrease algorithm and a Circuit Breaker pattern to dynamically scale requests, preventing IP bans and mitigating "Thundering Herd" problems.
* **Out-of-Order Execution to Sequential Stream:** Uses `asyncio.PriorityQueue` for LIFO retry handling and `heapq` as a sliding reordering buffer to convert chaotic concurrent HTTP ranges into a strict sequential byte stream.
* **Zero-Overhead UI Debouncing:** Uses a detached asynchronous refresh loop and a `defaultdict` buffer to batch terminal rendering operations, ensuring CPU load stays near 0% even at speeds of 500+ MB/s.
* **Crash-Proof State Persistence:** Uses `NamedTemporaryFile` and POSIX directory `fsync` to guarantee atomic state saves. If power is lost mid-save, the state file never corrupts.
* **Smart File Discovery:** Implements RFC 5987 parsing for `Content-Disposition` headers to extract complex UTF-8 filenames, falling back to URL parsing and mimetype guessing.
* **Graceful Shutdown & Fail-Fast:** Intercepts `SIGINT`/`SIGTERM`, safely flushes queues with Poison Pills (`-1`), and instantly cancels all worker tasks for a specific file if a fatal HTTP 404/403 is encountered.
---

## 🗺️ Roadmap

The journey of the Hydra has just begun. Here is what is planned for the future:

* **v1.1: Autonomous Worker Scaling (AIMD)**
  * Evolve the static thread pool into an adaptive concurrency manager. The system will dynamically spawn or kill download workers based on real-time network health and downstream pipeline backpressure. No more manual `--threads` tuning—the Hydra will automatically grow or shed heads to match your system's optimal capacity.
* **v2.0: Rewrite It In Rust (RIIR) 🦀**
  * Port the core engine to Rust using `tokio` and `reqwest` to bypass the Python GIL. This will enable true multi-core execution, zero-cost abstractions, and bare-metal performance for hashing and I/O, while maintaining a Python wrapper (`PyO3`) for seamless Data Science / ML integration.

---
## License
MIT License. Feel free to use, modify, and distribute.
