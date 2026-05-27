# List vs Sequence vs Flow in Kotlin

Based on the presentation **"Scaling Data Processing in Kotlin without Melting Your Memory"**

## Overview

This demo compares three approaches for processing large datasets:

| Approach | Evaluation | Blocking | Parallelization |
|----------|------------|----------|-----------------|
| **List** | Eager | Yes (`Thread.sleep`) | No |
| **Sequence** | Lazy | Yes (`Thread.sleep`) | No |
| **Flow** | Lazy | No (`delay`) | Yes (`flatMapMerge`) |

## Duration Explained

```
Query delay = 200ms per MMF (Amundi, DWS, Blackrock)
```

| Approach | Duration | Calculation |
|----------|----------|-------------|
| **List** | ~690ms | `200ms + 200ms + 200ms` + processing |
| **Sequence** | ~710ms | `200ms + 200ms + 200ms` + processing |
| **Flow** | ~380ms | `max(200ms, 200ms, 200ms)` + processing |

### Why?

- **List/Sequence**: Use `Thread.sleep(200ms)` which **BLOCKS** the thread. Must run sequentially.
- **Flow**: Uses `delay(200ms)` which **SUSPENDS**. `flatMapMerge(3)` starts all 3 queries at once.

```
List/Sequence:     |--Amundi 200ms--|--DWS 200ms--|--Blackrock 200ms--|

Flow:              |--Amundi 200ms--|
                   |--DWS 200ms-----|
                   |--Blackrock 200ms--|
                   ^ all start together!
```

## Memory Explained

| Approach | Memory | Why |
|----------|--------|-----|
| **List** | ~130MB | 80k customers × ~1KB each + intermediate lists from `.filter().map()` |
| **Sequence** | ~180MB | Same data, but `chunked()` creates temporary `List` for each chunk |
| **Flow** | ~220MB | Highest - coroutine overhead + all 3 MMF streams active simultaneously |

### Why Flow uses more memory?

All 3 MMF queries run **concurrently** = 3× the data in flight at peak, plus coroutine machinery overhead.

### The real Sequence benefit (short-circuiting)

```kotlin
// With take() - Sequence processes only what's needed:
sequence.filter{}.map{}.take(100)  // processes ~100 items, STOPS

// List processes ALL then discards:
list.filter{}.map{}.take(100)      // processes 80k items, keeps 100
```

## Scaling to Millions of Records

With **millions of data** the differences become critical:

### Memory at Scale

| Approach | 1 Million customers × 1KB | What happens |
|----------|---------------------------|--------------|
| **List** | ~1GB per MMF in memory | Likely **OutOfMemoryError** |
| **Sequence + chunked(10k)** | ~10MB at a time | Works fine |
| **Flow** | Streams, controlled memory | Works fine |

```kotlin
// LIST - tries to load 1 million into memory at once
val customers = db.fetchCustomersByMmfAsList(mmf)  // 1GB in heap!
    .filter { ... }  // creates ANOTHER 1GB list
    .map { ... }     // creates ANOTHER 1GB list
// Total: ~3GB just for intermediate collections!

// SEQUENCE - only holds 10k at a time
db.fetchCustomersByMmfAsSequence(mmf)
    .filter { ... }   // no intermediate collection
    .map { ... }      // no intermediate collection
    .chunked(10_000)  // only 10k in memory
    .forEach { chunk -> persist(chunk) }
```

### Speed at Scale

Speed difference stays similar (~2x for Flow) because it's about **parallelizing I/O**, not data size:

```
List/Sequence: 200ms + 200ms + 200ms = 600ms (sequential)
Flow:          max(200ms, 200ms, 200ms) = 200ms (parallel)
```

### Summary by Data Size

| Data Size | List | Sequence | Flow |
|-----------|------|----------|------|
| 100k | Works | Works | Fastest |
| 1M | Slow, high memory | Works, chunked | Fastest |
| 10M+ | **OOM crash** | Works | Fastest |

## When to Use Each

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Small data, need all results | **List** | Simplest, fastest for small data |
| Large data, need subset (`.take()`) | **Sequence** | Short-circuits, stops early |
| Large data, batch processing | **Sequence + chunked** | Memory control, one chunk at a time |
| Multiple I/O operations | **Flow + flatMapMerge** | Parallelism, non-blocking |

## Code Structure

```
listSequenceFlow/
├── FakeDatabase.kt    # Simulates DB with List, Sequence, Flow fetch methods
├── DataProcessor.kt   # Demo comparing all three approaches
└── README.md          # This file
```

## Running the Demo

```bash
./gradlew run
```

## Key Takeaways

1. **List** is eager - creates intermediate collections at each step (`.filter()`, `.map()`)

2. **Sequence** is lazy - processes one item through entire pipeline before next item
   - Great for `.take(N)` - stops after N items
   - Great for `.chunked()` - batch processing with controlled memory

3. **Flow** is async - uses coroutines for non-blocking operations
   - `delay()` suspends instead of blocking
   - `flatMapMerge(N)` enables N concurrent operations
   - Perfect for parallel I/O (DB queries, API calls)

## Example Output

```
[1] LIST - loads ALL into memory at once:
    80000 customers in 691ms

[2] SEQUENCE + chunked(10000) - processes in batches:
    80000 customers in 711ms
    Chunks processed: 9

[3] FLOW + flatMapMerge(3) - concurrent queries:
    80000 customers in 381ms

Summary:
  List:     691ms  - ALL data in memory
  Sequence: 711ms  - 9 chunks of 10000
  Flow:     381ms  - 3 queries in PARALLEL

  Flow is 1.8x faster than List!
```
