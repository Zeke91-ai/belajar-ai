# Telegram Crawler — Panduan Penggunaan

Aplikasi ini mengambil pesan dari grup dan channel Telegram, lalu menyimpannya ke database SQLite (`crawl.db3`). Selain pesan, aplikasi juga menyimpan:

- **Data pengirim** (nama, username, apakah bot) dari setiap pesan.
- **Keanggotaan**: user mana saja yang pernah mengirim pesan di grup/channel mana, dan kapan pertama dan terakhir mereka menulis.
- **Detail grup**: jumlah anggota dan deskripsi.
- **Akun Anda sendiri** (akun yang dipakai untuk login), ditandai `is_self = 1`.

> **Catatan penting:** "anggota" di sini berarti user yang **pernah mengirim pesan** di grup tersebut. Telegram tidak mengizinkan semua daftar anggota grup diambil tanpa hak admin, jadi user yang hanya membaca atau tidak pernah menulis tidak akan tercatat.

---

## 1. Persiapan

Yang Anda butuhkan:

1. **Python 3.10 atau lebih baru.** Cek dengan membuka Command Prompt lalu ketik:
   ```
   python --version
   ```
2. **Akun Telegram** yang nomor teleponnya sudah terdaftar.
3. **API_ID dan API_HASH** dari Telegram:
   1. Buka https://my.telegram.org dan login dengan nomor Telegram Anda.
   2. Pilih **API development tools**.
   3. Isi formulir (nama aplikasi bebas, misalnya `crawler-saya`).
   4. Anda akan mendapatkan **App api_id** (angka) dan **App api_hash** (string huruf dan angka).

---

## 2. Siapkan file `.env`

File `.env` berisi kredensial Anda dan **tidak boleh** dibagikan atau di-upload ke GitHub.

Pastikan file `.env` ada di folder proyek, isinya seperti ini:

```
API_ID=12345678
API_HASH=abcdef0123456789abcdef0123456789
PHONE_NUMBER=+6281234567890

# Hanya jika akun Anda memakai verifikasi dua langkah (2FA)
TELEGRAM_2FA_PASSWORD=

# Opsional
# DB_PATH=C:\Users\SCC1202201146\Documents\Ngoding\telegramcrawl\crawl.db3
# LOG_LEVEL=INFO
```

Aturan penulisan:
- `PHONE_NUMBER` harus memakai format internasional diawali `+62` (tanpa spasi).
- Jangan beri tanda kutip pada nilainya.
- Jika akun tidak memakai 2FA, biarkan `TELEGRAM_2FA_PASSWORD=` kosong.

---

## 3. Instalasi

Buka **Command Prompt** atau **PowerShell**, lalu jalankan perintah berikut satu per satu:

```
cd C:\Users\SCC1202201146\Documents\Ngoding\telegramcrawl
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Setelah `.venv\Scripts\activate` dijalankan, awal baris perintah akan menampilkan `(.venv)`. Itu tandanya lingkungan virtual aktif.

Jika `.venv` sudah pernah dibuat, cukup jalankan `cd` dan `.venv\Scripts\activate` saja.

---

## 4. Login pertama kali (wajib)

Langkah ini hanya dilakukan sekali. Hasilnya disimpan di `sessions\crawler.session`, sehingga run berikutnya tidak meminta login lagi.

```
python auth.py
```

Yang akan terjadi:

1. Aplikasi meminta kode login. Telegram akan mengirim kode ke aplikasi Telegram Anda (bukan SMS, kecuali Telegram memilih SMS).
2. Ketik kode tersebut, lalu tekan Enter.
3. Jika akun memakai 2FA dan `TELEGRAM_2FA_PASSWORD` kosong, aplikasi akan meminta password. Ketik password Anda (tidak akan terlihat saat diketik, itu normal).
4. Jika berhasil, log menampilkan `Login successful; session saved`.

Untuk memastikan sesi valid, jalankan `python auth.py` lagi. Jika sesi sudah valid, aplikasi langsung selesai tanpa meminta apa-apa.

---

## 5. Menjalankan crawler

### Crawl satu atau beberapa grup/channel

```
python main.py --channels @namagrup
python main.py --channels @grup1,@channel2,https://t.me/namachannel
```

### Crawl dari file daftar

Buat file teks, misalnya `daftar_grup.txt`, dengan satu grup per baris:

```
# Baris yang diawali tanda # diabaikan
@grupkelas
@komunitasjakarta
https://t.me/channelberita
```

Lalu jalankan:

```
python main.py --channels-file daftar_grup.txt
```

### Opsi yang tersedia

| Opsi | Fungsi | Contoh |
|---|---|---|
| `--channels` | Daftar grup dipisah koma | `--channels @grup1,@grup2` |
| `--channels-file` | File berisi daftar grup | `--channels-file daftar_grup.txt` |
| `--limit` | Maksimal pesan per grup per tahap. Default `1000`. Isi `0` untuk tanpa batas. | `--limit 5000` |
| `--offset-date` | Mulai dari pesan sebelum tanggal ini (hanya berlaku saat crawl pertama) | `--offset-date 2025-01-01` |
| `--batch-size` | Jumlah pesan per transaksi database. Default `500`. | `--batch-size 200` |

Contoh lengkap:

```
python main.py --channels-file daftar_grup.txt --limit 0 --offset-date 2025-06-01
```

### Cara kerja dua tahap

Setiap grup dicrawl dalam dua tahap:

1. **Backfill**: mundur dari pesan terlama yang sudah tersimpan (atau dari `--offset-date`) sampai awal grup.
2. **Incremental**: maju dari pesan terbaru yang tersimpan untuk mengambil pesan baru.

Karena itu, Anda bisa menjalankan crawler setiap hari tanpa mengambil ulang pesan yang sudah ada.

---

## 6. Menghentikan crawler dengan aman

- Tekan **Ctrl+C** sekali. Crawler menyelesaikan batch yang sedang berjalan, menyimpannya, lalu berhenti.
- Tekan **Ctrl+C** lagi untuk berhenti paksa. Data yang sudah tersimpan tetap aman; crawl berikutnya akan melanjutkan dari titik terakhir.

---

## 7. Melihat hasil

Database berada di `crawl.db3`. Anda bisa membukanya dengan **DB Browser for SQLite** (file `crawl.sqbpro` sudah ada di folder proyek), atau dari terminal:

```
python -c "import sqlite3; c=sqlite3.connect('crawl.db3'); print(c.execute('SELECT title, participants_count FROM channels').fetchall())"
```

### Tabel utama

| Tabel | Isi |
|---|---|
| `channels` | Daftar grup/channel: judul, username, jumlah anggota, deskripsi |
| `users` | Data user: nama, username, apakah bot, dan `is_self = 1` untuk akun Anda |
| `user_chats` | Relasi user ↔ grup, dengan tanggal pesan pertama dan terakhir |
| `messages` | Isi pesan, pengirim, waktu, jumlah views dan forwards |
| `media_metadata` | Info file media: jenis, nama file, ukuran |
| `crawl_state` | Checkpoint setiap grup (untuk melanjutkan crawl) |

### Contoh pertanyaan (SQL)

**Akun Anda sendiri:**
```sql
SELECT user_id, first_name, username FROM users WHERE is_self = 1;
```

**Siapa saja yang pernah menulis di grup tertentu, diurutkan dari yang paling baru aktif:**
```sql
SELECT u.first_name, u.last_name, u.username, uc.last_message_utc
FROM user_chats uc
JOIN users u ON u.user_id = uc.user_id
JOIN channels c ON c.chat_id = uc.chat_id
WHERE c.title LIKE '%kelas%'
ORDER BY uc.last_message_utc DESC;
```

**Grup mana saja yang pernah diikuti (ditulis di) oleh user tertentu:**
```sql
SELECT c.title, uc.first_message_utc, uc.last_message_utc
FROM user_chats uc
JOIN channels c ON c.chat_id = uc.chat_id
WHERE uc.user_id = 123456789;
```

**Pesan dari seseorang di sebuah grup:**
```sql
SELECT m.date_utc, m.text
FROM messages m
WHERE m.chat_id = -1001234567890 AND m.sender_id = 123456789
ORDER BY m.date_utc;
```

**Ringkasan grup:**
```sql
SELECT title, username, participants_count, about FROM channels;
```

> `chat_id` grup bernilai negatif (misalnya `-1001234567890`). Lihat nilainya di tabel `channels`.

---

## 8. Membuka log

Log tersimpan di folder `logs\crawler.log` dan juga tampil di layar. File log berotasi otomatis setiap 10 MB dan menyimpan 5 cadangan.

```
type logs\crawler.log
```

---

## 9. Masalah yang sering terjadi

| Pesan | Penyebab | Solusi |
|---|---|---|
| `Missing required environment variable: API_ID` | `.env` belum ada atau isinya kosong | Lengkapi file `.env` (lihat bagian 2) |
| `API_ID must be a numeric value` | API_ID berisi huruf atau tanda kutip | Tulis angka saja tanpa tanda kutip |
| `Session is not authorized` | Belum login | Jalankan `python auth.py` |
| `Session is in use (lock file: ...)` | Crawler lain sedang berjalan, atau crawler sebelumnya berhenti mendadak | Pastikan tidak ada crawler lain. Jika tidak ada, hapus file `sessions\crawler.lock` lalu jalankan ulang |
| `FloodWait: Telegram requires XXXs` | Terlalu banyak permintaan ke Telegram | Tunggu. Crawler otomatis menunggu lalu melanjutkan. Kurangi `--limit` atau jumlah grup jika sering terjadi |
| `Login code was invalid or expired` | Kode salah atau sudah kedaluwarsa | Jalankan `python auth.py` lagi dan gunakan kode terbaru |
| `Two-step verification password is incorrect` | Password 2FA salah | Perbaiki `TELEGRAM_2FA_PASSWORD` di `.env` atau kosongkan dan ketik ulang saat diminta |
| `Could not read group details` | Telegram membatasi detail grup (biasanya grup privat) | Tidak fatal. Data pesan tetap tersimpan, hanya jumlah anggota dan deskripsi yang kosong |
| `database is locked` | Database sedang dibuka dan diubah oleh proses lain | Tutup DB Browser atau proses lain yang mengakses `crawl.db3` lalu coba lagi |

---

## 10. Catatan privasi dan penggunaan

- Aplikasi ini menyimpan nama, username, dan isi pesan orang lain. Gunakan hanya untuk grup atau channel yang Anda punya hak akses, dan untuk keperluan yang sah.
- Pastikan penggunaan Anda sesuai dengan [Ketentuan Layanan Telegram](https://telegram.org/tos) dan peraturan perlindungan data pribadi yang berlaku di Indonesia (termasuk UU Pelindungan Data Pribadi).
- Jangan membagikan file `.env`, folder `sessions\`, atau file `crawl.db3` jika berisi data pribadi orang lain. File `.session` setara dengan akses ke akun Telegram Anda.
- Jangan menjalankan crawler terlalu agresif untuk banyak grup sekaligus. Ini memicu FloodWait dan dapat berisiko membatasi akun Anda.

---

## Ringkasan perintah

```
cd C:\Users\SCC1202201146\Documents\Ngoding\telegramcrawl
.venv\Scripts\activate
python auth.py                                  (sekali, untuk login)
python main.py --channels @namagrup             (crawl satu grup)
python main.py --channels-file daftar_grup.txt  (crawl banyak grup)
```


---

## 11. UI Mirror (cara utama mulai sekarang)

Mulai sekarang Anda tidak perlu lagi membuka PowerShell. Cukup klik dua kali file `start_ui.bat`. Browser akan terbuka di `http://127.0.0.1:8765/`.

### Login

- Username: `admin`
- Password: `12345678`

Password hanya diminta sekali per sesi UI. Sesi berakhir jika Anda menekan **Logout** atau menutup jendela `start_ui.bat`. Password disimpan di database dalam bentuk hash, tidak dalam teks biasa. Disarankan mengganti password di atas karena terlalu mudah ditebak.

### Menjalankan dan menghentikan mirror

- **Start mirror**: menyalakan layanan yang merekam semua aktivitas akun Anda secara real time. Saat pertama kali jalan, mirror juga mengambil riwayat pesan yang belum tersimpan (catch-up).
- **Stop mirror**: menghentikan layanan dengan aman. Pesan yang sedang disimpan selesai dulu, lalu berhenti.

Status di pojok kanan atas: `running` (hijau), `starting`, atau `stopped` (merah).

### Apa yang direkam

- Pesan masuk dan keluar, di semua chat privat dan grup.
- Edit pesan. Isi lama tercatat di tab Live.
- Pesan yang dihapus. Pesannya tetap disimpan dan ditandai **dihapus**.
- Kejadian di grup: user bergabung, keluar, perubahan judul atau foto.
- Perubahan profil akun Anda: nama, username, dan foto atau bio (Telegram tidak selalu menyebut field yang berubah).

Catatan: beberapa penghapusan di chat privat tidak menyebutkan chat-nya. Dalam kasus itu aplikasi hanya menandai pesan jika cocok dengan satu chat saja. Jika tidak, kejadian dicatat sebagai `message_deleted_unresolved` di tab Live.

### Tab di UI

| Tab | Fungsi |
|---|---|
| **Live** | Aktivitas terbaru secara real time |
| **Pesan** | Cari pesan dengan filter grup, pengirim, kata kunci, tanggal, dan status (dihapus, diedit) |
| **Grup & Channel** | Daftar semua chat dengan judul, username, jumlah anggota, dan jumlah pesan |
| **Orang** | Daftar pengirim. Klik jumlah grup untuk melihat di grup mana saja orang itu aktif |
| **Ekspor** | Unduh pesan (mengikuti filter) atau aktivitas sebagai CSV |

### Media

- Semua file media diunduh ke folder `media\`, kecuali video yang melebihi batas ukuran.
- Batas ukuran video diatur di `.env` dengan `MAX_VIDEO_DOWNLOAD_MB`. Default-nya 100 MB.
- Video yang melewati batas hanya disimpan metadata-nya (`skipped_large_video`).
- Media yang sudah diunduh bisa dibuka dari tab Pesan dengan mengklik nama file.

### Masalah umum (tambahan)

| Pesan | Solusi |
|---|---|
| Status tetap `stopped` setelah Start | Lihat `logs\mirror_stdout.log` dan `logs\crawler.log`. Biasanya sesi belum login (jalankan `python auth.py` sekali) |
| Status `stale` | Mirror berhenti mendadak. Klik Start lagi |
| `Session is in use` | Pastikan `main.py` tidak sedang berjalan bersamaan dengan mirror |
| Media berstatus `failed` | Coba lagi nanti. Telegram kadang menolak unduhan sementara |

### Keamanan data

- Database `crawl.db3` dan folder `media\` berisi seluruh riwayat percakapan Anda beserta data orang lain. Simpan di komputer ini saja dan jangan dibagikan atau di-backup ke penyimpanan publik.
- UI hanya bisa diakses dari komputer ini (127.0.0.1), tidak dari jaringan lain.
