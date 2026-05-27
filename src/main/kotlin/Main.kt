package org.danikotlingdemos

import kotlinx.coroutines.runBlocking

/**
 *   ┌──────────┬────────┬───────────────┬──────────────────────────────────────────────┐
 *   │ Approach    │ Memory   │     Time                  │                  Key Point
 *   ├──────────┼────────┼───────────────┼──────────────────────────────────────────────┤
 *   │ List     │ 52MB   │ 240ms         │ Creates 3 intermediate lists of 100k objects │
 *   ├──────────┼────────┼───────────────┼──────────────────────────────────────────────┤
 *   │ Sequence │ 1MB    │ 5ms           │ Only processed 10 items to get 10 results    │
 *   ├──────────┼────────┼───────────────┼──────────────────────────────────────────────┤
 *   │ Flow     │ -      │ 78ms vs 539ms │ 6x faster with 20 concurrent I/O calls       │
 *   └──────────┴────────┴───────────────┴──────────────────────────────────────────────┘
 *
 */

fun main() = runBlocking {
    val processor = DataProcessor()
    processor.runDemo()
}
