# 🧬 NCBI Async Downloader

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/Zhukovetski/NCBI-Async-Downloader/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/NCBI-Async-Downloader/actions/workflows/tests.yml)

<p align="center">
  <img src="https://github.com/user-attachments/assets/95330961-9470-462d-a50f-cf1427d0cc2a" alt="NCBI Downloader Demo" width="800">
</p>

A high-performance, fault-tolerant, and streaming-capable downloader for genomics datasets (NCBI, EBI, etc.). Built with pure Python, `asyncio`, and `httpx`.

## 💡 The Problem vs. The Solution

**The Problem:** Standard tools like `wget` or `curl` use single TCP connections, resulting in extremely slow downloads for 100GB+ genomic files. IBM Aspera Connect is fast but requires proprietary clients, UDP port exceptions, and often gets blocked by corporate firewalls. Moreover, processing these files usually requires downloading them to disk first, leading to massive I/O bottlenecks.

**The Solution:** `NCBI-Async-Downloader` utilizes HTTP/2 multiplexing, concurrent chunk downloading, and intelligent rate limiting to max out your bandwidth over standard HTTPS (port 443). **Its killer feature is the Sequential Reordering Buffer**, which allows you to stream multi-threaded downloads directly into Unix pipelines (`stdout`) without touching the disk.

---

## ✨ Key Features

* 🚀 **Maximized Throughput:** Concurrent chunk downloading using `uvloop` and `httpx`.
* 🌊 **True In-Memory Streaming:** Downloads chunks asynchronously but yields them sequentially. Pipe terabytes of data directly into bioinformatics tools (zero disk I/O).
* 🛡️ **Bulletproof Reliability:**
  * **AIMD Rate Limiting & Circuit Breaker** to prevent IP bans.
  * **Exponential Backoff + Full Jitter** for network drops.
  * **Partial Chunk Commits:** If a connection drops, it saves the exact byte offset. You never lose progress.
* 🧬 **Bioinformatics Ready:** Automatically locates `md5checksums.txt` on NCBI FTP servers and performs on-the-fly or post-download integrity validation.
* 💾 **Atomic Writes:** Uses low-level `os.pwrite` to prevent Global Interpreter Lock (GIL) bottlenecks and race conditions during disk I/O.
* 📊 **Adaptive & Pipe-Safe CLI:**
  All UI and logs are strictly routed to `stderr`, meaning **every mode is 100% safe for Unix pipelines** (`stdout` is reserved strictly for binary data streams).
  * **Default:** Beautiful, dynamic UI powered by `Rich` (gradients, global ETA).
  * `--no-ui`: Plain text logs. Perfect for CI/CD, Docker, and log parsers.
  * `--quiet`: Dead silence in the console. Ideal for strict background cron jobs (logs are still saved to disk).

---

## 🛠 Installation

Currently, you can run it directly from the source. It is recommended to use `uv` or `poetry`.

```bash
# Recommended: Install globally via uv tool
uv tool install git+https://github.com/Zhukovetski/NCBI-Async-Downloader.git

# Or via pipx
pipx install git+https://github.com/Zhukovetski/NCBI-Async-Downloader.git
```

After installation, you can use the `ncbiloader` command directly from anywhere in your terminal:
```bash
ncbiloader "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```

---

## 🚀 Usage

### 1. Basic Download (Disk Mode)
Download a file using 20 concurrent connections:
```bash
ncbiloader "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
*(If you hit `Ctrl+C`, the state is saved. Rerunning the exact command will resume the download from the exact byte).*

### 2. Unix Pipeline Streaming (The Killer Feature) 💥
Download a compressed genome, decompress it in memory, and count the sequences—**without saving the archive to your hard drive**:

```bash
ncbiloader "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream --quiet | zcat | grep -c "^>"
```

### 3. Use as a Python Library (For Data Science / ML)
You can embed the loader into your PyTorch/Pandas pipelines using the asynchronous generator:

```python
import asyncio
from ncbiloader import NCBILoader

async def main():
    urls =["https://url1.gz", "https://url2.gz"]

    async with NCBILoader(threads=10, quiet=True) as loader:
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
```
