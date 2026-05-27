package org.danikotlingdemos

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlin.system.measureTimeMillis

/**
 * Demonstrates data processing patterns from the presentation
 * "Scaling Data Processing in Kotlin without Melting Your Memory"
 *
 * Shows three approaches:
 * 1. LIST - Eager evaluation, high memory usage
 * 2. SEQUENCE - Lazy evaluation, memory efficient, single-threaded
 * 3. FLOW - Async/reactive, enables concurrent processing
 */
class DataProcessor(
    private val db: FakeDatabase = FakeDatabase()
) {
    // =========================================================================
    // 1. LIST Processing - Eager, creates intermediate collections
    // =========================================================================
    /**
     * Processes eligible customers using List operations.
     *
     * Problem from presentation:
     * - .filter creates a new List in the Heap
     * - .map creates another new List
     * - .take(10) only keeps 10 results
     * Result: JVM allocated memory for 200k objects just to use 10!
     */
    fun processWithList(targetCount: Int = 10): ProcessingResult {
        printMemory("Before List processing")

        var processedCount = 0
        val results: List<ProcessedOrder>
        val timeMs = measureTimeMillis {
            results = db.fetchEligibleCustomersAsList()
                .filter { customer ->
                    processedCount++
                    customer.id % 2 == 0 // Some filter condition
                }
                .map { customer ->
                    processCustomer(customer)
                }
                .take(targetCount)
        }

        printMemory("After List processing")

        return ProcessingResult(
            approach = "List (Eager)",
            resultCount = results.size,
            itemsProcessed = processedCount,
            timeMs = timeMs,
            note = "Created multiple intermediate lists in memory!"
        )
    }

    /**
     * Processes customers per MMF sequentially using List.
     * This is the slow approach from the presentation - loads ALL data into memory.
     */
    fun processAllMmfsWithList(): ConcurrentProcessingResult {
        printMemory("Before List processing")

        var totalProcessed = 0
        val mmfCounts = mutableMapOf<MMF, Int>()

        val timeMs = measureTimeMillis {
            MMF.entries.forEach { mmf ->
                val customers = db.fetchCustomersByMmfAsList(mmf)
                val processed = customers
                    .filter { it.isEligible }
                    .map { processCustomer(it) }

                totalProcessed += processed.size
                mmfCounts[mmf] = processed.size
            }
        }

        printMemory("After List processing")

        return ConcurrentProcessingResult(
            approach = "List (Eager)",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs,
            note = "Loads ALL data into memory at once!"
        )
    }

    // =========================================================================
    // 2. SEQUENCE Processing - Lazy, memory efficient
    // =========================================================================
    /**
     * Processes eligible customers using Sequence operations.
     *
     * How it works (from presentation):
     * - Sequences process ONE item at a time through the entire pipeline
     * - No intermediate collections are created
     * - Processing stops as soon as we have enough results (short-circuit)
     *
     * Result: Drastically reduces temporary objects hitting the JVM Heap!
     */
    fun processWithSequence(targetCount: Int = 10, chunkSize: Int = 1000): ProcessingResult {
        printMemory("Before Sequence processing")

        var processedCount = 0
        val results: List<ProcessedOrder>
        val timeMs = measureTimeMillis {
            results = db.fetchEligibleCustomersAsSequence(fetchSize = chunkSize)
                .filter { customer ->
                    processedCount++
                    customer.id % 2 == 0
                }
                .map { customer ->
                    processCustomer(customer)
                }
                .take(targetCount)
                .toList()
        }

        printMemory("After Sequence processing")

        return ProcessingResult(
            approach = "Sequence (Lazy)",
            resultCount = results.size,
            itemsProcessed = processedCount,
            timeMs = timeMs,
            note = "Only processed $processedCount items to get $targetCount results (short-circuit!)"
        )
    }

    /**
     * Processes customers using Sequence with chunked batching.
     * Simulates the pattern from the presentation:
     *
     * ```kotlin
     * .fetchStream().asSequence()
     * .chunked(chunkSize = 10_000)
     * .forEach { chunk -> persistOrderBatch(chunk) }
     * ```
     */
    fun processWithSequenceChunked(chunkSize: Int = 10_000): ProcessingResult {
        printMemory("Before Sequence chunked processing")

        var chunksProcessed = 0
        var totalProcessed = 0
        val timeMs = measureTimeMillis {
            db.fetchEligibleCustomersAsSequence(fetchSize = chunkSize)
                .map { processCustomer(it) }
                .chunked(chunkSize)
                .forEach { chunk ->
                    chunksProcessed++
                    totalProcessed += chunk.size
                    // Simulates persistOrderBatch()
                }
        }

        printMemory("After Sequence chunked processing")

        return ProcessingResult(
            approach = "Sequence + Chunked",
            resultCount = totalProcessed,
            itemsProcessed = totalProcessed,
            timeMs = timeMs,
            note = "Processed in $chunksProcessed chunks of ~$chunkSize items each"
        )
    }

    // =========================================================================
    // 3. FLOW Processing - Async with concurrency
    // =========================================================================
    /**
     * Processes eligible customers using Flow operations.
     * Similar to Sequence but supports async operations.
     */
    suspend fun processWithFlow(targetCount: Int = 10): ProcessingResult {
        printMemory("Before Flow processing")

        var processedCount = 0
        val results: List<ProcessedOrder>
        val timeMs = measureTimeMillis {
            results = db.fetchEligibleCustomersAsFlow()
                .filter { customer ->
                    processedCount++
                    customer.id % 2 == 0
                }
                .map { customer ->
                    processCustomer(customer)
                }
                .take(targetCount)
                .toList()
        }

        printMemory("After Flow processing")

        return ProcessingResult(
            approach = "Flow (Async)",
            resultCount = results.size,
            itemsProcessed = processedCount,
            timeMs = timeMs,
            note = "Cold stream - doesn't run until collected"
        )
    }

    /**
     * Processes ALL MMFs concurrently using Flow + flatMapMerge.
     *
     * This is the key optimization from the presentation:
     * - Sequential processing: 30min + 2h + 3h = 5h30 total
     * - Concurrent processing: max(30min, 2h, 3h) = 3h total
     *
     * ```kotlin
     * mmfs.asFlow()
     *     .flatMapMerge(concurrency = 3) { mmf ->
     *         db.readLargeEligibleDataset(mmf)
     *     }
     *     .map { customer -> processLightweightAppLogic(customer) }
     *     .collect { result -> db.writeResult(result) }
     * ```
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun processAllMmfsConcurrently(concurrency: Int = 3): ConcurrentProcessingResult {
        printMemory("Before concurrent Flow processing")

        var totalProcessed = 0
        val mmfCounts = mutableMapOf<MMF, Int>()

        val timeMs = measureTimeMillis {
            MMF.entries
                .asFlow()
                .flatMapMerge(concurrency = concurrency) { mmf ->
                    db.fetchCustomersByMmfAsFlow(mmf)
                        .filter { it.isEligible }
                        .map { customer ->
                            mmf to processCustomer(customer)
                        }
                }
                .collect { (mmf, _) ->
                    totalProcessed++
                    mmfCounts[mmf] = (mmfCounts[mmf] ?: 0) + 1
                }
        }

        printMemory("After concurrent Flow processing")

        return ConcurrentProcessingResult(
            approach = "Flow + flatMapMerge(concurrency=$concurrency)",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs,
            note = "All $concurrency MMFs processed in parallel!"
        )
    }

    /**
     * Processes MMFs sequentially for comparison with concurrent approach.
     */
    suspend fun processAllMmfsSequentially(): ConcurrentProcessingResult {
        printMemory("Before sequential Flow processing")

        var totalProcessed = 0
        val mmfCounts = mutableMapOf<MMF, Int>()

        val timeMs = measureTimeMillis {
            MMF.entries
                .asFlow()
                .collect { mmf ->
                    db.fetchCustomersByMmfAsFlow(mmf)
                        .filter { it.isEligible }
                        .map { customer ->
                            processCustomer(customer)
                        }
                        .collect {
                            totalProcessed++
                            mmfCounts[mmf] = (mmfCounts[mmf] ?: 0) + 1
                        }
                }
        }

        printMemory("After sequential Flow processing")

        return ConcurrentProcessingResult(
            approach = "Flow (Sequential)",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs,
            note = "Each MMF processed one after another"
        )
    }

    // =========================================================================
    // Demo runner - MMF concurrent processing
    // =========================================================================
    suspend fun runDemo() {
        val stats = db.getStatistics()
        println("=".repeat(70))
        println("MMF Processing Demo: Sequential vs Concurrent Flow")
        println("=".repeat(70))
        println("Database stats:")
        println("  Total customers: ${stats.totalCustomers}")
        println("  Eligible customers: ${stats.eligibleCustomers}")
        stats.customersByMmf.forEach { (mmf, count) ->
            println("  ${mmf.displayName}: $count customers (${mmf.dataPercentage}%)")
        }
        println()

        println("-".repeat(70))
        println("Processing all MMFs (Sequential vs Concurrent):")
        println("-".repeat(70))

        println("\n[1] List (Eager) - loads all data into memory:")
        val listResult = processAllMmfsWithList()
        println("    Total: ${listResult.totalProcessed} customers in ${listResult.timeMs}ms")
        listResult.processedByMmf.forEach { (mmf, count) ->
            println("    - ${mmf.displayName}: $count")
        }

        println("\n[2] Sequential Flow - each MMF one after another:")
        val seqMmfResult = processAllMmfsSequentially()
        println("    Total: ${seqMmfResult.totalProcessed} customers in ${seqMmfResult.timeMs}ms")
        seqMmfResult.processedByMmf.forEach { (mmf, count) ->
            println("    - ${mmf.displayName}: $count")
        }

        println("\n[3] Concurrent Flow - flatMapMerge(concurrency=3):")
        val concMmfResult = processAllMmfsConcurrently(concurrency = 3)
        println("    Total: ${concMmfResult.totalProcessed} customers in ${concMmfResult.timeMs}ms")
        concMmfResult.processedByMmf.forEach { (mmf, count) ->
            println("    - ${mmf.displayName}: $count")
        }

        if (seqMmfResult.timeMs > 0 && concMmfResult.timeMs > 0) {
            val speedup = seqMmfResult.timeMs.toDouble() / concMmfResult.timeMs
            println()
            println("Speedup: %.1fx faster with concurrent processing!".format(speedup))
        }

        println()
        println("=".repeat(70))
    }

    // =========================================================================
    // Helper functions
    // =========================================================================
    private fun processCustomer(customer: Customer): ProcessedOrder {
        return ProcessedOrder(
            customerId = customer.id,
            mmf = customer.mmf,
            orderType = if (customer.id % 2 == 0) OrderType.BUY else OrderType.SELL,
            amount = customer.id * 100.0
        )
    }

    private fun printMemory(label: String) {
        System.gc()
        val runtime = Runtime.getRuntime()
        val used = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
        println("  [$label] Memory: ~${used}MB")
    }
}

// =============================================================================
// Result Models
// =============================================================================
data class ProcessingResult(
    val approach: String,
    val resultCount: Int,
    val itemsProcessed: Int,
    val timeMs: Long,
    val note: String
)

data class MmfProcessingResult(
    val mmf: MMF,
    val customersProcessed: Int,
    val durationMs: Long
)

data class ConcurrentProcessingResult(
    val approach: String,
    val totalProcessed: Int,
    val processedByMmf: Map<MMF, Int>,
    val timeMs: Long,
    val note: String
)
