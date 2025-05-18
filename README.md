# Primitive Search Engine

A simple search engine implementation featuring a **Web Crawler**, **Indexer**, and **Query Engine**.

---

## 🔍 Overview

This search engine consists of three main components:

1. **Spider / Web Crawler**  
   Discovers web pages using BFS, respects blacklists, supports concurrency, and can resume from its last state after interruptions.

2. **Indexer**  
   Builds an **Inverted Index** that maps words to documents (web pages). Supports resumable indexing or reindexing indexed pages.

3. **Query Engine**  
   Exposes a search API endpoint that handles user keyword queries and returns the most relevant web pages based on relevance scoring.

---

## 🚀 Getting Started

### 📡 Spider / Web Crawler

The web crawler:
- Accepts a seed list of URLs and a blacklist.
- Performs a BFS crawl without depth limits (currently).
- Runs concurrently for faster link extraction.
- Supports resumable crawling on abrupt stops.

#### Run the Crawler

```bash
python main.py crawler
```


### 🗂️ Indexer
The indexer:
- Builds an inverted index (mapping words to a list of documents).
- Currently single-threaded but resumable after a stop.

#### Run the Crawler

```bash
python main.py indexer
```

### 🔎 Query Engine

The query engine:

- Provides a RESTful API for searching.
- Accepts keyword queries and calculates relevance scores.
- Returns the most relevant web pages.

```bash
cd ./services/query-engine/
npm run dev
```

---

## ⚙️ Environment Variables
Create a ```.env``` file in the root directory of the project:
```env
DB_HOST=
DB_USER=
DB_PASSWORD=
DB_NAME=

QUERY_ENGINE_PORT=
```

## 📁 Config Directory

Create a `config/` folder in the root of your project, and add the following two text files:

### 🔹 `seed_urls.txt`
Contains the **initial set of seed URLs** the crawler will begin with.

Example:
```seed_urls.txt
https://en.wikipedia.org/
https://www.britannica.com/
```
### 🔹 `blacklist.txt`
Contains **domains or URLs** that the crawler should completely ignore.

Example:
```blacklist.txt
https://www.example.com
```