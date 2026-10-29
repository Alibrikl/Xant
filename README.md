<div align="center">

# ⚡ Xant - Media Downloader Userbot

**A high-performance asynchronous Telegram userbot designed to batch-download protected media from private channels.**

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telethon](https://img.shields.io/badge/Telethon-Async-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://docs.telethon.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

---

</div>

## 📌 Overview

**Xant** is an automated Telegram Userbot script powered by [Telethon](https://github.com/LonamiWebs/Telethon). It bypasses content protection restrictions (`has_protected_content`) on private Telegram channels where saving or forwarding media is prohibited.

Simply forward or paste post links into your **Saved Messages**, and **Xant** will process them in sequence, download the files directly from Telegram servers, and send them back to you.

---

## ✨ Features

- 🔓 **Bypass Restricted Content:** Download photos, videos, and documents from channels with restricted saving/forwarding.
- 📦 **Batch Processing:** Send multiple post links in a single message - Xant will queue and process them one by one.
- 📊 **Real-Time Progress Tracking:** Shows downloading percentage, transferred data (MB), and remaining total size.
- 🛡️ **Network Resilience:** Built-in connection retry mechanism to handle TCP disconnects and network drops gracefully.
- ⏱️ **Anti-FloodWait Protection:** Smart status update throttling (2-second interval) to comply with Telegram API rate limits.
- 🧹 **Auto-Cleanup:** Automatically deletes downloaded temporary files from storage once delivered.

---

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Alibrikl/Xant.git
cd Xant
```
### 2. Install Dependencies
```bash
pip install -r requirements.txt
```
### 3. Standard Run
```bash
python xant.py
```
### 3.1 Running on Android (Termux)
```bash
pkg update && pkg upgrade -y
pkg install python git -y
git clone https://github.com/Alibrikl/Xant.git
cd Xant
pip install -r requirements.txt
python xant.py
```

---

## ⚙️ Configuration & First Launch

Xant features an interactive initial setup. You don't need to manually create or edit any environment files before running the project.

### 1. Get Your API Credentials

1. Log in with your Telegram phone number at [my.telegram.org](https://my.telegram.org).
2. Go to **API development tools**.
3. Create a new application (fill in any title and short name, e.g., `XantApp`).
4. Copy your **`api_id`** and **`api_hash`**.

---

### 2. Run Initial Setup

Start the userbot using Python:

```bash
python xant.py
```
On your first run, Xant will prompt you to enter your API credentials directly in the terminal:
```bash
==================================================
🚀 Welcome to Xant Userbot Setup!
Get your API credentials from https://my.telegram.org
==================================================
Enter your TELEGRAM_API_ID:
Enter your TELEGRAM_API_HASH:
```

---

## 📖 How to Use

1. **Start the Userbot:** Launch `Xant` on your local PC, Termux, or server.
2. **Open Saved Messages:** In your Telegram app, go to your **Saved Messages** (`me`).
3. **Send Links:** Paste one or multiple Telegram post links (one per line or separated by spaces):

```text
https://t.me/c/1234567890/100 <- example
https://t.me/c/1234567890/101 <- example
https://t.me/c/1234567890/102 <- example
```
---

## ⚠️ Disclaimer

> **IMPORTANT NOTICE:** This software is provided strictly for educational, research, and personal archiving purposes only.

* **Compliance with Terms of Service:** Users are solely responsible for ensuring that their use of **Xant** complies with Telegram’s [Terms of Service](https://telegram.org/tos) and relevant local regulations.
* **Copyright & Content Protection:** Do not use this tool to download, duplicate, or redistribute copyrighted media without explicit permission from the original content creator or rights holder.
* **Limitation of Liability:** The author/developer assumes **no liability or responsibility** for any account bans, suspensions, data loss, or legal consequences resulting from the deployment or misuse of this script.

*By running this project, you acknowledge and accept full legal responsibility for your actions.*
