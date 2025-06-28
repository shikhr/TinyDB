# Custom C++ File-Based DBMS

## Project Setup

This project is a custom, file-based RDBMS in C++, inspired by SQLite. It uses a single file to store data and supports basic SQL commands.

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
