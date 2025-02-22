# BynxDB

A minimal database from scratch using Go. I wanted to understand how databases actually work under the hood, which turned into a fascinating deep-dive into database internals.

## Features I've Implemented

### Fixed sized Page division 
I implemented a Data Access Layer (DAL) which is responsible for dividing the database files into fixed size chunks and make managing individual data nodes feasilble. 

### B Trees for Indexing
I chose B Trees because they're practically the industry standard for database indexes. My implementation includes:

Efficient range queries
Auto-balancing on insert/delete
Disk-friendly node structure
Configurable node size

### Learning Resources
If you're interested in building your own database, here are some resources I found incredibly helpful:

[Build Your Own book](https://build-your-own.org/)
