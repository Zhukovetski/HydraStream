# 🐉 HydraStream

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml)

<p align="center">
  <img src="https://github.com/user-attachments/assets/95330961-9470-462d-a50f-cf1427d0cc2a" alt="HydraStream Demo" width="800">
</p>

A high-performance, fault-tolerant, and streaming-capable downloader for Big Data. Built with pure Python, `uvloop`, and `httpx`.

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
* 🧩 **Smart Integrity Validation:** Automatically extracts and verifies MD5 checksums from AWS S3 (`ETag`), Google Cloud (`x-goog-hash`), standard HTTP headers, and NCBI provider files.
* 💾 **Atomic Writes:** Uses low-level `os.pwrite` to prevent Global Interpreter Lock (GIL) bottlenecks during disk I/O.
* 📊 **Adaptive UI:**
  * **Default:** Beautiful, dynamic terminal UI powered by `Rich`.
  * `-nu / --no-ui`: Plain text logs for CI/CD environments.
  * `-q / --quiet`: Strict POSIX compliance (stderr for logs, stdout for data streams).

---

## 🛠 Installation

Requires Python 3.11+. Install globally using `uv` (recommended) or `pipx`:

```bash
uv tool install git+https://github.com/Zhukovetski/HydraStream.git

pipx install git+https://github.com/Zhukovetski/HydraStream.git
```

After installation, you can use the `hydrastream` command directly from anywhere in your terminal:
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
*(If interrupted, rerun the exact command to resume from the last saved byte).*

### 2. Unix Pipeline Streaming (The Killer Feature) 💥
Download a compressed 100GB file, decompress it in memory, and process it—**without saving the archive to your disk**:

```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream --quiet | zcat | grep -c "^>"
```

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
| `--buffer` | `-b` | `threads * 5120`| Maximum stream buffer size in bytes. |
---

## 🧠 Under the Hood (Architecture)

For those interested in System Design, this tool implements:
* **Domain-Driven Design:** Separation of concerns across `network.py`, `storage.py`, `models.py`, and `loader.py`.
* **Out-of-Order Execution to Sequential Stream:** Uses `asyncio.PriorityQueue` for LIFO retry handling and `heapq` as a reordering buffer to convert chaotic concurrent HTTP ranges into a strict sequential byte stream.
* **Graceful Shutdown:** Intercepts `SIGINT`/`SIGTERM`, safely flushes queues with Poison Pills (`-1`), and commits partial chunks to prevent zombie tasks or memory leaks.

---
## License
MIT License. Feel free to use, modify, and distribute.
