# BUGS.md - Bug Log

Log bug nyata yang ditemukan & diperbaiki selama proses build.

---

## Bug #1: Pydantic Schema Syntax Error pada Field `$schema`
**Tanggal**: 2026-09-14
**Lokasi**: `producer/schema.py:22`
**Masalah**: Field `$schema` menggunakan `$` di nama variabel Python, yang tidak valid.
**Error**: `SyntaxError: invalid syntax`
**Perbaikan**: Mengganti `$schema` dengan `schema_field` dan menggunakan `Field(alias="$schema")`
**Status**: ✅ Fixed

---

## Bug #2: SSE Client Missing Async Context Manager
**Tanggal**: 2026-09-14
**Lokasi**: `producer/main.py`
**Masalah**: `SSEClient` tidak mengimplementasikan proper cleanup pada exception, menyebabkan resource leak.
**Perbaikan**: Menambahkan method `disconnect()` dan memanggilnya di `finally` block
**Status**: ✅ Fixed

---

## Bug #3: Consumer DuckDB Connection Race Condition
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/main.py`
**Masalah**: Consumer dan Aggregator menulis ke DuckDB bersamaan tanpa coordination, potensial locking issues.
**Perbaikan (saat itu)**: 
- Consumer menggunakan INSERT ... ON CONFLICT DO UPDATE untuk idempotency
- Aggregator menggunakan CREATE OR REPLACE TABLE yang atomik
**Status**: ⚠️ Analisis awal tidak lengkap — lihat Bug #32. Klaim "DuckDB concurrent reads aman (MVCC), writes tapi serialized" di versi awal entry ini **salah** untuk mode embedded single-file DuckDB: yang sebenarnya terjadi adalah exclusive file lock per proses (bukan MVCC lintas proses), dan itu baru benar-benar menyebabkan kegagalan nyata setelah end-to-end run (Bug #32). Idempotency INSERT...ON CONFLICT dan CREATE OR REPLACE TABLE tetap dipakai (benar dan tetap berguna), tapi tidak cukup tanpa perbaikan pola koneksi di Bug #32.

---

## Bug #4: Exponential Backoff Producer Reconnect Logic
**Tanggal**: 2026-09-14
**Lokasi**: `producer/main.py`
**Masalah**: Backoff tidak reset setelah koneksi sukses beberapa kali, menyebabkan delay tidak perlu.
**Perbaikan**: Reset backoff ke BASE setelah `await sse_client.connect()` sukses
**Status**: ✅ Fixed

---

## Bug #5: Keyword Matching Multiple Additions
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/scoring.py`
**Masalah**: Loop keyword berhenti setelah `break` tapi variabel `found_keyword` tidak digunakan (dead code). Sementara itu logika benar karena `break` sudah memastikan hanya 1x penambahan.
**Perbaikan**: Menghapus variabel `found_keyword` yang tidak digunakan
**Status**: ✅ Fixed

---

## Bug #6: IP Address Regex Validation
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/scoring.py`
**Masalah**: Regex IPv4 menerima angka >255 per octet (misal: `999.999.999.999`).
**Perbaikan**: Menambahkan validasi numeric range (0-255) setelah regex match
**Status**: ✅ Fixed

---

## Bug #7: Event Time Parsing di Consumer
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/main.py`
**Masalah**: `message.get("time")` bisa jadi `None` atau format tidak standar, menyebabkan crash saat `datetime.fromisoformat()`.
**Perbaikan**: Menambahkan fallback dan try/except di `buffer_bronze` dan `flush_bronze`
**Status**: ✅ Fixed

---

## Bug #8: Edit Warring Count Threshold
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/scoring.py` & `tests/test_scoring.py`
**Masalah**: Spesifikasi ">3 kali" diimplementasikan sebagai `>= 4` (karena >3 = 4+), tapi test awal salah mengasumsikan `>= 3`.
**Perbaikan**: Update test dan kode agar konsisten: `>= 4` edits dalam 5 menit
**Status**: ✅ Fixed

---

## Bug #9: Parquet Partition Path Handling
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/main.py`
**Masalah**: `Path.mkdir(parents=True, exist_ok=True)` tapi path partition berisi `/` yang bisa menyebabkan error pada beberapa OS.
**Perbaikan**: Menggunakan `partition_path.mkdir(parents=True, exist_ok=True)` secara eksplisit
**Status**: ✅ Fixed

---

## Bug #10: Dashboard Table Not Ready
**Tanggal**: 2026-09-14
**Lokasi**: `dashboard/app.py`
**Masalah**: Dashboard crash saat gold tables belum dibuat oleh aggregator.
**Perbaikan**: Menambahkan `check_gold_tables()` dan `st.rerun()` dengan delay
**Status**: ✅ Fixed

---

## Bug #11: Bot Score Lock
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/scoring.py`
**Masalah**: Logika `bot == true` harus mengembalikan 0 SEBELUM memeriksa kondisi lain, tapi di kode awal dieksekusi terakhir.
**Perbaikan**: Memindahkan check `bot` ke paling awal function
**Status**: ✅ Fixed

---

## Bug #12: Missing Import in Producer
**Tanggal**: 2026-09-14
**Lokasi**: `producer/main.py`
**Masalah**: Import `Optional` tidak digunakan tapi type hint `Optional[httpx.AsyncClient]` diperlukan.
**Perbaikan**: Memastikan import lengkap
**Status**: ✅ Fixed

---

## Bug #13: SSE Header Parsing
**Tanggal**: 2026-09-14
**Lokasi**: `producer/main.py`
**Masalah**: `parse_sse_event` hanya memproses baris yang dimulai dengan `data:`, mengabaikan `event:` dan `id:` fields.
**Analisis**: Untuk Wikimedia EventStreams, field `data` adalah payload utama yang berisi seluruh event JSON. Field lain bersifat metadata stream.
**Keputusan**: Implementation saat ini sudah cukup untuk kasus penggunaan.
**Status**: ✅ Verified Working

---

## Bug #14: Consumer Batch Size & Flush Interval
**Tanggal**: 2026-09-14
**Lokasi**: `consumer/main.py`
**Masalah**: Buffer Parquet flush berdasarkan `BATCH_SIZE` ATAU `BATCH_FLUSH_INTERVAL`. Di kode awal logika `should_flush` menggunakan `OR` tapi lupa reset `last_bronze_flush` di semua path.
**Perbaikan**: Memastikan `last_bronze_flush = time.time()` di-set setelah flush di semua kondisi
**Status**: ✅ Fixed

---

## Bug #15: Aggregator Scheduler Startup Delay
**Tanggal**: 2026-09-14
**Lokasi**: `aggregator/main.py`
**Masalah**: `BlockingScheduler` dengan `IntervalTrigger(seconds=60)` tidak menjalankan job segera pada startup (hanya setelah 60 detik pertama).
**Perbaikan**: Menambahkan `self.aggregate()` manual call sebelum `scheduler.start()`
**Status**: ✅ Fixed

---

## Bug #16: Data Directory Permissions in Container
**Tanggal**: 2026-09-14
**Lokasi**: `docker-compose.yml`, `producer/Dockerfile`, `consumer/Dockerfile`, `aggregator/Dockerfile`, `dashboard/Dockerfile`
**Masalah**: Directory `/app/data` perlu writable oleh semua container (mounted dari host).
**Perbaikan**: Menambahkan `RUN mkdir -p /app/data` di Dockerfile dan mount volume di docker-compose
**Status**: ✅ Fixed

---

## Bug #17: Python Version Compatibility
**Tanggal**: 2026-09-14
**Lokasi**: Semua Dockerfile
**Masalah**: Python 3.11 required, tapi base image `python:3.11-slim` OK. `httpx` dan `sseclient-py` compatibility check passed.
**Status**: ✅ Verified

---

## Bug #18: Missing Test Coverage for Scoring Edge Cases
**Tanggal**: 2026-09-14
**Lokasi**: `tests/test_scoring.py`
**Masalah**: Test awal tidak mencakup semua 8 kasus wajib (vandalism, legit, bot, threshold=50, IP on Talk, edit warring, empty comment large change, combination).
**Perbaikan**: Menulis ulang test dengan 12 test cases lengkap
**Status**: ✅ Fixed

---

## Bug #19: Aggregator DuckDB Query Syntax
**Tanggal**: 2026-09-14
**Lokasi**: `aggregator/main.py`
**Masalah**: DuckDB `NOW()` function tidak support `INTERVAL` syntax secara langsung di beberapa versi.
**Perbaikan**: Menggunakan `date_trunc` dan manual datetime calculation
**Status**: ✅ Fixed (using DuckDB 1.1.1 syntax)

---

## Bug #21: Kesalahan Diagnosis "Wikimedia Memblokir Docker Container"
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py`
**Masalah**: Saat mendiagnosis kegagalan koneksi ke `https://stream.wikimedia.org/v2/stream/recentchange` dari dalam container, dilakukan test manual dengan `httpx.get(url)` (tanpa `stream=True`). Untuk endpoint SSE yang tidak pernah menutup response body, `.get()` biasa menunggu response selesai (yang tidak pernah terjadi) sehingga terlihat seperti hang/gagal.
**Diagnosis awal (SALAH)**: Kegagalan ini disimpulkan sebagai "Wikimedia memblokir IP Docker container" — kesimpulan ini keliru, murni artefak dari metode testing yang salah (pakai `.get()` alih-alih `.stream()` untuk endpoint yang tidak berujung).
**Perbaikan yang sempat diambil (dan dibatalkan)**: Berdasarkan diagnosis salah tersebut, sumber data sempat diganti total dari SSE ke Wikimedia REST API (`https://en.wikipedia.org/w/api.php`) dengan polling (`WikimediaAPIPoller`, `WikimediaEventTransformer`, dll).
**Perbaikan final (BENAR)**: Setelah pengujian ulang dengan `httpx.AsyncClient.send(..., stream=True)` (mode streaming yang benar untuk SSE) terbukti berhasil connect ke endpoint SSE dan menerima data live (HTTP 200). Producer dikembalikan sepenuhnya ke implementasi SSE asli; seluruh kode REST API polling dihapus (lihat Bug #25).
**Status**: ✅ Fixed — root cause adalah kesalahan metode testing, bukan pemblokiran jaringan. Implementasi final tetap SSE (`producer/main.py`), sesuai spesifikasi di AGENTS.md.

---

## Bug #22: Boolean Parsing Error for `minor` Field
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py`
**Masalah**: Wikimedia API mengembalikan field `minor` sebagai string kosong (`""`) untuk edit non-minor, tapi Pydantic schema mengharapkan boolean.
**Error**: `Input should be a valid boolean, unable to interpret input [type=bool_parsing, input_value='', input_type=str]`
**Perbaikan**: Menambahkan `bool()` coercion di `validate_event()`:
```python
if "minor" in data and data["minor"] is not None:
    data["minor"] = bool(data["minor"])
```
**Status**: ✅ Fixed

---

## Bug #23: Redpanda v24.2.2 Incompatible CLI Flags
**Tanggal**: 2026-09-15
**Lokasi**: `docker-compose.yml`
**Masalah**: Redpanda image v24.2.2 menggunakan CLI flags yang berbeda dengan v26.1.17.
**Error**: `unknown flag: --rpc-server`
**Perbaikan**: Upgrade ke Redpanda v26.1.17 dan menggunakan command `["redpanda", "start", "--mode", "dev-container"]` yang kompatibel.
**Status**: ✅ Fixed

---

## Bug #24: Docker Build Context Path Errors
**Tanggal**: 2026-09-15
**Lokasi**: Semua Dockerfile
**Masalah**: COPY commands menggunakan path relatif yang tidak valid dari build context.
**Error**: `failed to calculate checksum: "/requirements.txt": not found`
**Perbaikan**: Mengubah COPY paths menjadi relatif terhadap build context root (`./producer/`, `./consumer/`, dst.)
**Status**: ✅ Fixed

---

## Bug #25: Dead Code from Aborted REST API Approach
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py`
**Masalah**: Setelah kembali ke SSE implementation, code dari pendekatan REST API (WikimediaAPIPoller, transform_api_to_sse_format, API_PARAMS, rcshow) masih ada sebagai dead code.
**Perbaikan**: Menghapus semua code terkait REST API polling:
- Hapus class `WikimediaAPIPoller`
- Hapus class `WikimediaEventTransformer`
- Hapus `API_PARAMS` dict
- Hapus `WIKIMEDA_API_URL` konstanta
- Hapus `API_POLL_INTERVAL` konstanta
- Hapus `rcshow` referensi
**Verifikasi**: `grep -rn "WikimediaAPIPoller\|rcshow\|API_PARAMS\|transform_api_to_sse_format" producer/` menghasilkan output kosong.
**Status**: ✅ Fixed

---

## Bug #26: Sisa Dead Code Lain Ditemukan Saat Audit Ulang
**Tanggal**: 2026-09-15
**Lokasi**: `consumer/main.py`, `producer/requirements.txt`
**Masalah**: Audit ulang menemukan sisa kode/dependency yang tidak pernah dipakai, tidak terkait langsung dengan percobaan REST API tapi tetap dead code:
- `WikipediaConsumer.add_to_recent_edits()` (async, dengan `asyncio.Lock`) tidak pernah dipanggil — logikanya sudah diduplikasi secara sinkron di `process_event()` karena consumer loop bersifat sinkron.
- `cleanup_bronze()` dan `run_cleanup_loop()` di `consumer/main.py` tidak pernah dipanggil dari `__main__` — cleanup bronze retention sudah jadi tanggung jawab `aggregator/main.py` sesuai spesifikasi.
- `producer/requirements.txt` mencantumkan `sseclient-py` padahal implementasi final hanya memakai `httpx` (streaming mode).
**Perbaikan**: Menghapus `add_to_recent_edits`, `recent_edits_lock`, `cleanup_bronze`, `run_cleanup_loop`, konstanta `MAX_BRONZE_RETENTION_DAYS` yang tak terpakai di consumer, import `asyncio` yang jadi tak terpakai, dan dependency `sseclient-py`.
**Status**: ✅ Fixed

---

## Bug #27: Producer Reconnect Loop Tanpa Backoff Memicu 403 dari Wikimedia
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py`
**Masalah**: Setelah `docker compose up --build` dari clean state, producer terus menerima `403 Forbidden` dari `https://stream.wikimedia.org/v2/stream/recentchange`, dengan retry beruntun setiap ~80-90ms (bukan mengikuti backoff 1-60 detik yang seharusnya).
**Root cause**: `SSEClient.connect()` menangkap semua exception secara internal dan mengembalikan `False`, tidak pernah raise. Di `run_producer()`, `if await sse_client.connect():` yang gagal (False) langsung membuat loop `while True` mengulang dari awal tanpa pernah masuk blok `except` — padahal logika exponential backoff (`asyncio.sleep(backoff)`) ada di situ. Akibatnya producer membanjiri Wikimedia dengan request tanpa jeda, dan Wikimedia merespons dengan 403 (rate-limit/anti-abuse), bukan pemblokiran IP Docker secara umum.
**Verifikasi manual**: `docker compose exec producer` menjalankan request SSE manual dengan `httpx.Client.stream()` (metode streaming yang benar) berhasil dapat `HTTP 200` dan menerima event live — membuktikan endpoint tidak diblokir, masalah murni di sisi retry loop.
**Perbaikan**: Menambahkan `else: raise ConnectionError(...)` setelah `if await sse_client.connect():` gagal, sehingga kegagalan connect masuk ke blok `except` yang sudah ada dan backoff diterapkan dengan benar.
**Status**: ✅ Fixed

---

## Bug #28: Redpanda Crash (OOM) karena Shard Count Mengikuti Jumlah Core Host
**Tanggal**: 2026-09-15
**Lokasi**: `docker-compose.yml`
**Masalah**: Container Redpanda crash beberapa menit setelah start dengan log `seastar - Failed to allocate 16384 bytes`, lalu restart otomatis (`restart: unless-stopped`) berulang.
**Root cause**: Redpanda mode `dev-container` secara default membuat 1 shard per CPU core host. Di mesin dengan 16 core, itu berarti 16 shard berbagi `mem_limit: 2G` (~124MB/shard efektif), sehingga alokasi memori gagal di bawah beban sedang. Ini murni fungsi dari jumlah core host, bukan volume data — artinya perilaku tidak reproducible/predictable di mesin lain.
**Perbaikan**: Menambahkan flag eksplisit `--smp 1 --memory 1900M` ke command Redpanda supaya jumlah shard tetap konsisten (1 shard) di mesin manapun — cukup untuk single-node portfolio project ini, tetap dalam batas `mem_limit: 2G`, dan berada di atas ambang "recommended memory per shard" (1GB) milik Redpanda sendiri (dengan `--smp 2` sempat muncul warning "Memory below recommended" karena memori terbagi 2 shard).
**Status**: ✅ Fixed

---

## Bug #29: Producer Kirim Request SSE Tanpa Header User-Agent (Root Cause Asli 403)
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py` (`SSEClient.connect()`)
**Masalah**: Setelah Bug #27 diperbaiki (backoff bekerja), producer tetap menerima `403 Forbidden` dari Wikimedia di **setiap** percobaan, walau test manual (`httpx.Client(...).stream()` sinkron dari dalam container yang sama, momen yang sama) berhasil dapat `200`.
**Root cause**: `connect()` membuat `httpx.AsyncClient(timeout=None, headers=headers)` (header `User-Agent` di-set di level client), tapi request yang dikirim dibuat manual lewat `httpx.Request("GET", self.url)` lalu dikirim via `client.send(request, ...)`. Ternyata `client.send()` pada objek `Request` mentah **tidak** menggabungkan header default milik client (perilaku ini berbeda dari `client.get()`/`client.stream()` yang otomatis melalui `client.build_request()`). Dibuktikan dengan mencetak `dict(req.headers)` sebelum dikirim: hasilnya cuma `{'host': 'stream.wikimedia.org'}` — tidak ada `User-Agent` maupun `Accept` sama sekali. Wikimedia menolak request tanpa User-Agent deskriptif dengan 403, persis seperti yang diperingatkan di AGENTS.md.
**Perbaikan**: Meneruskan `headers=headers` langsung ke `httpx.Request(...)` (bukan hanya ke `AsyncClient(...)`), sehingga request yang benar-benar dikirim membawa `User-Agent` dan `Accept: text/event-stream`.
**Status**: ✅ Fixed

---

## Bug #30: Redpanda Advertise `127.0.0.1:9092`, Tidak Terjangkau dari Container Lain
**Tanggal**: 2026-09-15
**Lokasi**: `docker-compose.yml`
**Masalah**: Producer/consumer berhasil bootstrap ke `redpanda:9092`, tapi request produce/consume berikutnya gagal: `Connect to ipv4#127.0.0.1:9092 failed: Connection refused`.
**Root cause**: Mode `dev-container` Redpanda secara default advertise Kafka API sebagai `127.0.0.1:9092` ke client (asumsi client jalan di host yang sama). Protokol Kafka mengharuskan client memakai alamat "advertised listener" ini untuk request setelah bootstrap awal — tapi `127.0.0.1` di dalam container producer/consumer merujuk ke diri mereka sendiri, bukan ke container Redpanda.
**Perbaikan**: Menambahkan flag `--advertise-kafka-addr redpanda:9092` supaya Redpanda advertise hostname docker-network (`redpanda`) yang bisa di-resolve dari semua container di network yang sama.
**Status**: ✅ Fixed

---

## Bug #31: `validate_event()` Asumsikan Envelope CloudEvents yang Tidak Ada di Stream Asli
**Tanggal**: 2026-09-15
**Lokasi**: `producer/main.py` (`validate_event()`), `producer/schema.py`
**Masalah**: Setelah Bug #29 diperbaiki (koneksi SSE berhasil, HTTP 200, data live diterima), producer tetap **tidak pernah mempublish satu event pun** — 100% event gagal validasi dengan `Invalid isoformat string: ''`, lalu setelah itu diperbaiki, gagal lagi dengan `minor: Field required` dan `log_params: Input should be a valid dictionary`.
**Root cause**: `validate_event()` mengasumsikan payload berbentuk envelope ala CloudEvents (`{"type", "topic", "source", "specversion", "id", "time", "data": {...}, "meta": {...}}`) dengan field recentchange asli tersimpan di `data`. Payload asli dari `stream.wikimedia.org/v2/stream/recentchange` **tidak** dibungkus begitu — field recentchange (`id`, `type`, `title`, `timestamp`, dst.) ada langsung di top level, hanya `meta` yang nested. Akibatnya `event.get("data", {})` selalu `{}` dan `event.get("time", "")` selalu string kosong, sehingga parsing datetime crash untuk setiap event. Selain itu, dua masalah struktural lain baru kelihatan setelah fix pertama diterapkan:
- Field `minor` memang tidak ada untuk recentchange bertipe `log`/`categorize`/`external` (bukan `edit`) — bukan bug data, memang begitu skema resminya.
- Field `id` top-level tidak ada untuk event bertipe `log` (dipakai `log_id` sebagai gantinya).
- Field `log_params` bisa berupa list ATAU dict tergantung `log_action`, bukan selalu dict.
**Perbaikan**:
- `validate_event()` sekarang membangun `data` dari seluruh field top-level event (minus `meta`), dan `time` diambil dari `meta.dt` (bukan top-level `time` yang tidak ada).
- `EventData.minor` diberi default `False` untuk tipe non-edit.
- Fallback `id` ke `log_id`, lalu ke `meta.id` kalau top-level `id` tidak ada, supaya setiap event tetap punya identifier unik untuk Kafka key & primary key DuckDB.
- `EventData.log_params` diubah ke `Optional[Any]` supaya menerima list maupun dict.
**Verifikasi**: Setelah fix, consumer log menunjukkan event terus diproses (`Processed 100/200/300... events`) tanpa lagi ada warning `Event validation failed`.
**Status**: ✅ Fixed

---

## Bug #32: DuckDB File Lock Exclusive — Consumer/Dashboard Menahan Koneksi Permanen, Aggregator Tidak Pernah Kebagian
**Tanggal**: 2026-09-15
**Lokasi**: `consumer/main.py`, `dashboard/app.py`
**Masalah**: Setelah pipeline berhasil mempublish & memproses event (Bug #29, #31 fixed), aggregator **tetap** gagal setiap siklus 60 detik: `IO Error: Could not set lock on file "/app/data/wikipedia.duckdb": Conflicting lock is held in PID 0`. Menjalankan perintah verifikasi manual (`docker compose exec consumer python -c "import duckdb; ..."`) juga gagal dengan error lock yang sama — membuktikan masalahnya nyata dan reproducible, bukan cuma di aggregator.
**Root cause**: DuckDB (mode embedded single-file, bukan client/server) hanya mengizinkan **satu** proses OS memegang file terbuka pada satu waktu (either banyak reader read-only, atau satu writer — tidak bisa campur). Sebelum fix:
- `consumer/main.py` membuka SATU koneksi DuckDB di awal (`_init_duckdb()`) dan menahannya terbuka selama seluruh siklus hidup service (dipakai untuk setiap insert per-message), sehingga secara praktis memegang exclusive lock 100% dari waktu consumer berjalan.
- `dashboard/app.py` melakukan hal yang sama lewat `@st.cache_resource` — koneksi read-write dibuka sekali dan di-cache selama seluruh sesi Streamlit, menahan lock permanen juga.
Baik aggregator (yang butuh CREATE OR REPLACE TABLE setiap 60 detik) maupun proses verifikasi ad-hoc manapun praktis tidak pernah punya kesempatan mendapatkan lock file tersebut.
**Perbaikan**:
- `consumer/main.py`: schema dibuat sekali lewat koneksi singkat (`_init_duckdb_schema()`), lalu penulisan silver diubah dari insert langsung per-message menjadi **buffer + flush periodik** (`buffer_silver()`/`flush_silver()`, mengikuti pola `BATCH_SIZE`/`BATCH_FLUSH_INTERVAL` yang sudah ada untuk bronze) — koneksi DuckDB hanya dibuka sesaat saat flush, lalu langsung ditutup, bukan ditahan sepanjang hidup service.
- `dashboard/app.py`: `get_duckdb_connection()` tidak lagi di-cache dengan `@st.cache_resource`; dibuka fresh dalam mode `read_only=True` sekali per rerun Streamlit (~5 detik), dipakai untuk semua query di rerun tersebut, lalu ditutup eksplisit di `finally` sebelum `time.sleep()`/`st.rerun()` berikutnya.
**Verifikasi**: Setelah fix ini, gold tables mulai terisi data (sebelumnya selalu kosong/error). Tapi retest lanjutan menunjukkan masih ada sisa race condition intermiten — lihat Bug #33.
**Status**: ⚠️ Fixed sebagian — pola buffer+flush mengurangi drastis waktu lock ditahan, tapi belum menghilangkan tabrakan sepenuhnya karena sekarang ada 3 proses (consumer, aggregator, dashboard) yang sama-sama membuka/menutup koneksi secara periodik ke file yang sama. Perbaikan lanjutan di Bug #33.

---

## Bug #33: Tabrakan Lock DuckDB Masih Terjadi Sesekali Meski Sudah Buffer+Flush (Bug #32)
**Tanggal**: 2026-09-15
**Lokasi**: `consumer/main.py`, `aggregator/main.py`, `dashboard/app.py`
**Masalah**: Setelah Bug #32 diperbaiki, retest clean-state menunjukkan aggregator **masih** gagal 2x berturut-turut (`Conflicting lock is held in PID 0`) di siklus 60-detiknya, dan perintah verifikasi manual `SELECT COUNT(*) FROM enriched_edits` yang dijalankan 30 detik setelah check pertama juga kena lock error yang sama — walau log consumer sendiri menunjukkan siklus flush-nya bersih (tidak ada `Failed to write to DuckDB`, flush terjadi tiap ~2.5-3.5 detik dan selesai cepat).
**Root cause**: Dengan pola buffer+flush di Bug #32, jendela waktu lock ditahan consumer sudah jauh lebih pendek, tapi **bukan nol** — dan sekarang bukan cuma 2 proses (consumer+aggregator) yang berebut file yang sama, tapi 3: consumer (flush tiap ~3 detik), aggregator (tiap 60 detik), dan dashboard (buka koneksi read-only tiap rerun ~5 detik). DuckDB embedded file hanya mengizinkan satu proses pegang file dalam satu waktu (baik itu satu writer, ATAU banyak reader — tidak bisa campur read-only dan read-write bersamaan). Dengan 3 aktor independen yang masing-masing punya siklus pendek, peluang dua di antaranya mencoba connect di momen yang sama menjadi cukup signifikan secara statistik, walau masing-masing individual window singkat.
**Perbaikan**: Menambahkan `connect_duckdb_with_retry()` (helper kecil identik di ketiga file — retry dengan jittered backoff, maksimum 5 percobaan) di semua titik `duckdb.connect(...)`: `consumer._init_duckdb_schema()` & `flush_silver()`, `aggregator.connect()`, dan `dashboard.get_duckdb_connection()`. Hanya retry kalau pesan error mengandung "lock" (bukan menutupi error lain yang genuinely fatal).
**Status**: ✅ Fixed untuk ketiga service (aggregator 0 error di seluruh log retest; consumer tidak pernah gagal flush). **Catatan penting**: perintah verifikasi ad-hoc dari luar (`docker compose exec consumer python -c "duckdb.connect(...)"`, tanpa retry karena bukan bagian dari aplikasi) masih bisa sesekali kena lock transient — pada retest, 1 dari 5 percobaan ad-hoc gagal, sisanya sukses dengan angka yang naik terus. Ini bukan bug aplikasi, melainkan properti inheren DuckDB single-writer-file yang dijelaskan di atas; dicatat sebagai Known Limitation di README.

---

## Bug #34: Race Kondisi Startup — Consumer Subscribe Sebelum Topic Dibuat Producer
**Tanggal**: 2026-09-15
**Lokasi**: `consumer/main.py` (bukan bug kode, perilaku Kafka/Redpanda normal)
**Masalah**: Log consumer menunjukkan satu kali error di awal startup: `Consumer error: KafkaError{code=UNKNOWN_TOPIC_OR_PART,...}: Subscribed topic not available: wikipedia.recentchange.raw`.
**Root cause**: Consumer subscribe ke topic sebelum producer sempat mempublish message pertama (topic di Redpanda dibuat otomatis saat message pertama dipublish, bukan saat subscribe). Ini murni race startup yang wajar antar container yang start bersamaan.
**Dampak**: Tidak ada — `consumer.poll()` di `run()` sudah menangani `message.error()` dengan log+continue (tidak crash), dan begitu producer berhasil publish message pertama, topic otomatis tersedia dan consumer lanjut normal. Terjadi tepat 1 kali di seluruh log retest, tidak berulang.
**Perbaikan**: Tidak diperlukan perubahan kode — perilaku ini sudah di-handle dengan benar oleh loop consumer yang ada. Dicatat di sini murni untuk dokumentasi kejadian nyata sesuai permintaan AGENTS.md.
**Status**: ℹ️ Informational — bukan bug, self-healing by design.