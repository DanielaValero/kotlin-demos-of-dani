package org.danikotlingdemos

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlin.system.measureTimeMillis

class CollectionProcessor(
    private val itemCount: Int = 100_000,
    private val networkDelayMs: Long = 50L
) {
    // =========================================================================
    // 1. LIST - Creates intermediate collections at each step = HIGH MEMORY
    // =========================================================================
    fun processWithList() {
        printMemory("Before")

        val time = measureTimeMillis {
            val result = (1..itemCount)
                .map { HeavyObject(it) }           // 100k HeavyObjects in memory
                .map { it.copy(data = it.data.uppercase()) }  // Another 100k
                .filter { it.id % 2 == 0 }         // 50k filtered copy
                .take(10)
                .toList()

            println("Result: ${result.size} items (first: ${result.first().id})")
        }

        printMemory("After")
        println("Time: ${time}ms")
        println("⚠️  Notice: Multiple intermediate lists created in memory!")
    }

    // =========================================================================
    // 2. SEQUENCE - Lazy, processes one item through entire pipeline at a time
    // =========================================================================
    fun processWithSequence() {
        printMemory("Before")

        var itemsProcessed = 0
        val time = measureTimeMillis {
            val result = (1..itemCount)
                .asSequence()
                .map { HeavyObject(it) }
                .map { it.copy(data = it.data.uppercase()) }
                .filter { it.id % 2 == 0 }
                .onEach { itemsProcessed++ }
                .take(10)
                .toList()

            println("Result: ${result.size} items (first: ${result.first().id})")
        }

        printMemory("After")
        println("Items actually processed: $itemsProcessed (not $itemCount!)")
        println("Time: ${time}ms")
        println("✅ Lazy: Only processed what was needed, memory stayed flat!")
    }

    // =========================================================================
    // 3. FLOW - Async processing with concurrency for I/O operations
    // =========================================================================
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun processWithFlow() {
        // First, show that Sequence is SLOW with blocking I/O
        println("\n  [3a] Sequence with simulated network calls (BLOCKING):")
        val sequenceTime = measureTimeMillis {
            val result = (1..100)  // Only 100 items - sequence can't parallelize!
                .asSequence()
                .map { simulateNetworkCallBlocking(it) }
                .filter { it.id % 2 == 0 }
                .take(5)
                .toList()
            println("       Result: ${result.size} items")
        }
        println("       Time: ${sequenceTime}ms (sequential, ~${sequenceTime / 5}ms per item)")

        // Now show Flow with concurrency
        println("\n  [3b] Flow with concurrency (PARALLEL I/O):")
        val flowTime = measureTimeMillis {
            val result = (1..100)
                .asFlow()
                .flatMapMerge(concurrency = 20) { id ->  // 20 concurrent requests!
                    flow { emit(simulateNetworkCall(id)) }
                }
                .filter { it.id % 2 == 0 }
                .take(5)
                .toList()
            println("       Result: ${result.size} items")
        }
        println("       Time: ${flowTime}ms (parallel, 20x faster!)")

        println("\n  Speedup: ${sequenceTime / flowTime}x faster with Flow concurrency!")
        println("  ✅ Flow parallelizes I/O - perfect for DB/API calls!")
    }

    // =========================================================================
    // Supporting code
    // =========================================================================

    private fun simulateNetworkCallBlocking(id: Int): HeavyObject {
        Thread.sleep(networkDelayMs)  // Blocking sleep
        return HeavyObject(id)
    }

    private suspend fun simulateNetworkCall(id: Int): HeavyObject {
        delay(networkDelayMs)  // Non-blocking suspend
        return HeavyObject(id)
    }

    private fun printMemory(label: String) {
        System.gc()  // Hint to GC for more accurate readings
        val runtime = Runtime.getRuntime()
        val used = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
        println("$label memory: ~${used}MB")
    }
}

data class HeavyObject(
    val id: Int,
    val data: String = "Item_$id" + "x".repeat(1000)  // ~1KB per object
)
