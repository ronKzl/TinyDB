# TinyDB

TinyDB is a relational database engine built from scratch in **Go**. This project serves as a deep dive into database internals, focusing on the mapping of structured relational data to low-level key-value storage.

### Core Features
* **Relational Storage Engine**
* **Order-Preserving Serialization**
* **Custom SQL Parser**
* **Index-Backed Range Scans**
* **Write-Ahead Logging (WAL)**

### Current Capabilities
TinyDB supports standard CRUD operations and relational definitions via a command-line interface:
* **CREATE TABLE**: Define schemas with typed columns (`int64`, `string`) and composite primary keys.
* **INSERT / UPSERT**: Record insertion into the Key-Value store.
* **SELECT / UPDATE / DELETE**: Targeted operations using primary key lookups or sorted range-based iteration.

### Engineering Roadmap
The architecture is currently transitioning from an in-memory store to a disk system:
1.  **Range queries**: Implementing the ability to select on more than just equality operator.
2.  **LSM-Tree Architecture**: Moving toward Log-Structured Merge-Trees to optimize write throughput and minimize disk I/O.
3.  **Concurrency Control**: Implementing MVCC (Multi-Version Concurrency Control) or basic locking mechanisms to allow safe, concurrent access.
4.  **ACID Compliance**: Formalizing atomicity and durability through improved WAL recovery and transaction management.

### Technical References
* **System Architecture**: *Database Internals: A Deep Dive into How Distributed Data Systems Work* by Alex Petrov.
* **Language Specification**: [The Go Programming Language Specification](https://go.dev/ref/spec).
