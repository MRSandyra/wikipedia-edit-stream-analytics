# Wikipedia Edit Stream Analytics

## Overview & Motivation

Wikipedia Edit Stream Analytics adalah project portfolio data engineering yang memonitor firehose edit publik Wikipedia secara real-time. Project ini mengimplementasikan pipeline streaming end-to-end yang:

- Mengonsumsi data event edit Wikipedia secara langsung dari EventStreams API
- Menerapkan scoring berbasis aturan yang canggih untuk mendeteksi vandalism dan aktivitas mencurigakan
- Menyimpan data terstruktur dalam format Parquet (bronze layer) dan DuckDB (silver layer)
- Menghasilkan tabel agregasi denormalized (gold layer) untuk dashboard
- Menyediakan dashboard live yang menunjukkan volume edit, suspicious feeds, dan rasio bot/human

Ini adalah project lengkap yang dijalankan secara lokal dengan seluruh pipeline streaming data end-to-end, menggunakan Docker dan teknologi Python modern.

## Arsitektur

```mermaid
graph TB
    subgraph "Source"
        Wikimedia[.wikimedia.org/stream]
    end
    
    subgraph "Producer"
        Producer[Producer Service]
        Producer --> Redpanda
    end
    
    subgraph "Broker"
        Redpanda["Redpanda (Kafka API)"]
        Broker1["topic: wikipedia.recentchange.raw"]
        Redpanda --- Broker1
    end
    
    subgraph "Consumer"
        Consumer[Consumer Service]
        Consumer --> Broker1
        
        subgraph "Processing"
            Scoring[Rule-Based Scoring]
            DuckDB[Sink DuckDB - enriched_edits]
            Parquet[Sink Parquet - bronze]
        end
    end
    
    subgraph "Aggregator"
        Aggregator[Aggregator Service]
        Aggregator --> DuckDB
        
        subgraph "Gold Tables"
            Volume1min[gold_edit_volume_1min]
            TopPages15min[gold_top_pages_15min]
            SuspiciousFeed[gold_suspicious_feed]
            BotHumanRatio[gold_bot_human_ratio_1min]
        end
    end
    
    subgraph "Dashboard"
        Dashboard[Streamlit Dashboard]
        Dashboard --> Volume1min
        Dashboard --> TopPages15min
        Dashboard --> SuspiciousFeed
        Dashboard --> BotHumanRatio
    end
    
    Wikimedia --> Producer
    Consumer --> Aggregator
    Aggregator --> Dashboard
    
    style Producer fill:#e3f2fd,stroke:#2196f3
    style Consumer fill:#f3e5f5,stroke:#9c27b0
    style Aggregator fill:#e8f5e8,stroke:#4caf50
    style Dashboard fill:#fff3e0,stroke:#ff9800
    style Redpanda fill:#fff8e1,stroke:#ffc107
```

## Setup & Run

### Prasyarat

- Docker Desktop (Docker Engine v20.10+)
- Docker Compose v2+
- Memori RAM: minimal 16GB (disarankan 32GB untuk kinerja optimal)

### Penyiapan

1. **Clone repository** (jika belum):
   ```bash
   git clone <repository-url>
   cd wikipedia-edit-stream-analytics
   ```

2. **Jalankan semua service**:
   ```bash
   docker compose up --build
   ```

3. **Akses dashboard**:
   - Buka browser dan kunjungi `http://localhost:8501`
   - Dashboard akan mulai refresh otomatis setiap 5 detik

4. **Hentikan semua service**:
   ```bash
   docker compose down
   ```

### Pilihan SSE Client: `httpx`

AGENTS.md mengizinkan `httpx` (streaming mode) atau `sseclient-py`. Project ini memakai **`httpx.AsyncClient`** (`response.aiter_lines()` pada request dengan `stream=True`) karena:
- Sudah async-native, cocok dengan `asyncio` event loop yang dipakai producer untuk reconnect logic.
- `sseclient-py` bersifat sinkron/blocking, sehingga butuh thread terpisah untuk tidak memblokir loop async — kompleksitas ekstra yang tidak dibutuhkan.
- `httpx` juga dipakai untuk logic reconnect (exponential backoff) tanpa dependency tambahan.

### Estimasi RAM per Service

| Service | RAM per container | Total RAM (max) | Catatan |
|---------|------------------|-----------------|--------|
| Redpanda | 2 GB | 2 GB | Broker Kafka, penyimpanan data streaming |
| Producer | 512 MB | 512 MB | SSE client, Kafka producer, validasi Pydantic |
| Consumer | 1 GB | 1 GB | Kafka consumer, scoring rules, write DuckDB/Parquet |
| Aggregator | 512 MB | 512 MB | APScheduler, DuckDB queries, cleanup jobs |
| Dashboard | 512 MB | 512 MB | Streamlit, Plotly, DuckDB reads |
| **Total** | **~4 GB** | **4 GB** | Jauh di bawah 16GB batas |

## Suspicion Score Rules

Sistem scoring rule-based menghitung suspicion score (0-100) untuk setiap edit. Sebuah edit dianggap **mencurigakan** jika score >= 50.

| Kondisi | Poin | Keterangan |
|-----------|------|-------------|
| **Pengurangan konten besar**: `length.new - length.old < -500` | +30 | Edit menghapus lebih dari 500 byte |
| **Editor anonim (IPv4/IPv6)** mengedit namespace selain Talk | +15 | IP addresses dianggap anonim |
| **Comment kosong** pada perubahan besar (`|length.new - length.old| > 1000`) | +10 | Comment kosong dengan perubahan ukuran signifikan |
| **Comment mengandung kata kunci** dari `config/suspicious_keywords.yaml` (case-insensitive) | +25 | Deteksi keywords vandalism/spam |
| **User yang sama** mengedit halaman yang sama >3 kali dalam window 5 menit terakhir | +20 | Pola edit-warring |
| **Bot == true** | Score dikunci ke 0 | Bot tidak pernah ditandai suspicious |

**Bobot maksimum**: total skor dibatasi maksimal 100.

## Dashboard Features

### KPI Row
- **Edit Rate**: Edits per detik (rolling 1 menit)
- **Total Edits**: Cumulative edit sejak service start
- **Active Wikis**: Number of different wikis yang aktif dalam 5 menit terakhir
- **Suspicious %**: Persentase edit yang ditandai suspicious

### Charts
- **Edit Volume**: Line chart editted volume per wiki (30 menit terakhir)
- **Top Pages**: Bar chart top 10 halaman paling banyak diedit (15 menit terakhir)
- **Suspicious Feed**: Tabel 50 edit suspicious terbaru dengan page links
- **Bot vs Human**: Ratio perbandingan edit bot vs human per menit

## Screenshot

![Dashboard Screenshot](docs/screenshot-dashboard.png)

*(Placeholder — ganti dengan screenshot dashboard aktual setelah `docker compose up --build` berjalan minimal beberapa menit dan menampilkan data live.)*

## Known Limitations

1. **Koneksi SSE**: Stream Wikimedia bisa mengalami timeout; sistem menggunakan exponential backoff (1-60 detik) tapi bisa kehilangan event singkat saat reconnect.

2. **I/O Sink Storage**: Batasan write performance untuk Parquet files bisa menjadi bottleneck saat volume edit tinggi (terutama pada bursts vandalism).

3. **Delay Aggregator**: Job agregasi terjadwal (interval 60 detik) introduces ~1 menit delay antara edit dan munculnya di dashboard.

4. **Kapasitas Scoring**: Rule-based scoring sederhana bisa memiliki false positives untuk pattern edit legitimate (misalnya, sejumlah besar edit kecil dari user yang sama).

5. **Namespace Check**: Deteksi IP di namespace Talk menggunakan hardcoded check; tidak mencakup namespace khusus (misalnya, MediaWiki, Template).

6. **Cleanup Jobs**: Cleanup bronze files berjalan sebagai bagian dari siklus aggregator (tiap 60 detik), jadi file lebih tua dari `MAX_BRONZE_RETENTION_DAYS` tidak langsung terhapus di detik itu juga, hanya di siklus aggregator berikutnya.

7. **DuckDB Single-Writer File Lock**: DuckDB (mode embedded, bukan client/server) hanya mengizinkan satu proses memegang file database dalam satu waktu. Consumer, aggregator, dan dashboard semuanya membuka koneksi secara periodik dan singkat (bukan menahan koneksi permanen — lihat BUGS.md Bug #32/#33) dengan retry+backoff otomatis, sehingga dalam praktiknya jarang gagal. Tapi query ad-hoc dari luar aplikasi (mis. `docker compose exec consumer python -c "duckdb.connect(...)"` untuk debugging manual) tidak punya retry tersebut dan **bisa sesekali kena `Conflicting lock`** jika persis bertabrakan dengan salah satu proses di atas — cukup diulang beberapa detik kemudian.

## File Configuration

- `.env.example`: Template environment variables
- `config/suspicious_keywords.yaml`: Kata kunci untuk mendeteksi vandalism
- `docker-compose.yml`: Definisi service dan resource limits

## Monitoring

- **Producer**: Log status koneksi SSE, events yang diformat, dan publish Kafka
- **Consumer**: Logging activity batch, DuckDB writes, dan buffer parquet
- **Aggregator**: Log completion job agregasi dan file cleanup
- **Dashboard**: Built-in Streamlit logging dan error handling

Untuk debug, lihat container logs:
```bash
docker compose logs -f [service-name]
```

## Pengembangan Lanjutan

Saran untuk peningkatan:

1. **ML-based Detection**: Ganti/add scoring rules dengan classifier yang diawasi
2. **Stream Processing**: Gunakan Apache Flink atau Apache Beam untuk streaming processing
3. **WebSocket Dashboard**: Real-time updates tanpa page refresh
4. **Alert System**: Kirim notification ke Slack/Email saat vandalism terdeteksi
5. **Multi-source**: Tambahkan Firehose API lain (misalnya, MediaWiki API)
6. **More sophisticated NLP**: Gunakan embeddings semantic untuk comment analysis

## Lisensi

Project ini untuk tujuan portfolio. Lisensi tergantung pada contributor.