# TinyDB | C++ File-Based DBMS

## TinyDB Setup

TinyDB is a simple file-based RDBMS in C++, inspired by SQLite and PostgreSQL. It uses a single file to store data and supports basic SQL commands.

### Build & Run

```bash
mkdir build && cd build
cmake ..
make
./bin/tinydb
```

### Tests

```bash
cd build
cmake ..
make
ctest
```

## Roadmap

### Phase 1: Core Storage Engine

- [x] **Project Setup**: Configure CMake, Git, and Google Test.
- [x] **Disk Manager**: Handles low-level, page-based file I/O.
- [x] **Page & Buffer Pool Manager**: Manages caching of disk pages in memory.

### Phase 2: Data & Execution Layer

- [x] **System Catalog**: Manages metadata about tables and schemas.
- [x] **Table Heap & Record Manager**: Implements storage for unordered rows on pages.
- [x] **SQL Parser**: Translates SQL strings into an Abstract Syntax Tree (AST).
- [ ] **Execution Engine**: Executes queries using a sequential scan plan.

### Phase 3: Indexing & Advanced Queries

- [ ] **B+ Tree Index**: A generic B+ Tree implementation for indexing.
- [ ] **Index Manager**: Integrates B+ Trees as secondary indexes.
- [ ] **Query Optimizer**: A basic optimizer to choose between table scans and index scans.
- [ ] **Advanced Executors**: Support for `UPDATE`, `DELETE`, and `WHERE` clauses.

### Phase 4: Concurrency & Recovery (Future Goals)

- [ ] **Transaction Manager**: Manages ACID properties for transactions.
- [ ] **Lock Manager**: Provides concurrency control using locking.
- [ ] **Log Manager**: Implements Write-Ahead Logging (WAL) for recovery.
