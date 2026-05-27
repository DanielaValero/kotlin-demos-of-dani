package org.danikotlingdemos.listSequenceFlow

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlin.system.measureTimeMillis

/**
 * Demonstrates data processing patterns from the presentation
 * "Scaling Data Processing in Kotlin without Melting Your Memory"
 *
 * Three approaches:
 * 1. LIST - Eager, loads all into memory, blocking
 * 2. SEQUENCE - Lazy, memory efficient, blocking
 * 3. FLOW - Async, non-blocking, enables parallelization
 */
class DataProcessor(
    private val db: FakeDatabase = FakeDatabase()
) {

    /**
     * List - loads ALL data into memory at once.
     */
    fun processAllMmfsWithList(): ProcessingResult {
        printMemory("Before List")

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

        printMemory("After List")

        return ProcessingResult(
            approach = "List",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs
        )
    }

    /**
     * Sequence with .chunked() - processes in batches.
     * Memory efficient: only one chunk in memory at a time.
     */
    fun processAllMmfsWithSequence(chunkSize: Int = 10_000): ProcessingResult {
        printMemory("Before Sequence")

        var totalProcessed = 0
        var chunksProcessed = 0
        val mmfCounts = mutableMapOf<MMF, Int>()

        val timeMs = measureTimeMillis {
            MMF.entries.forEach { mmf ->
                db.fetchCustomersByMmfAsSequence(mmf)
                    .filter { it.isEligible }
                    .map { processCustomer(it) }
                    .chunked(chunkSize)
                    .forEach { chunk ->
                        chunksProcessed++
                        totalProcessed += chunk.size
                        mmfCounts[mmf] = (mmfCounts[mmf] ?: 0) + chunk.size
                    }
            }
        }

        printMemory("After Sequence")

        return ProcessingResult(
            approach = "Sequence + chunked($chunkSize)",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs,
            chunksProcessed = chunksProcessed
        )
    }

    /**
     * Flow with flatMapMerge - concurrent queries.
     * All 3 MMF queries run in PARALLEL!
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun processAllMmfsWithFlowConcurrent(concurrency: Int = 3): ProcessingResult {
        printMemory("Before Flow")

        var totalProcessed = 0
        val mmfCounts = mutableMapOf<MMF, Int>()

        val timeMs = measureTimeMillis {
            MMF.entries
                .asFlow()
                .flatMapMerge(concurrency = concurrency) { mmf ->
                    db.fetchCustomersByMmfAsFlow(mmf)
                        .filter { it.isEligible }
                        .map { customer -> mmf to processCustomer(customer) }
                }
                .collect { (mmf, _) ->
                    totalProcessed++
                    mmfCounts[mmf] = (mmfCounts[mmf] ?: 0) + 1
                }
        }

        printMemory("After Flow")

        return ProcessingResult(
            approach = "Flow + flatMapMerge",
            totalProcessed = totalProcessed,
            processedByMmf = mmfCounts.toMap(),
            timeMs = timeMs
        )
    }

    suspend fun runDemo() {
        val stats = db.getStatistics()
        val chunkSize = 10_000

        println("=".repeat(70))
        println("MMF Processing: List vs Sequence (chunked) vs Flow (concurrent)")
        println("=".repeat(70))
        println("Database: ${stats.totalCustomers} customers")
        println("Query delay: 200ms per MMF (simulates DB latency)")
        println()

        println("-".repeat(70))

        System.gc()
        println("\n[1] LIST - loads ALL into memory at once:")
        val listResult = processAllMmfsWithList()
        printResult(listResult)

        System.gc()
        println("\n[2] SEQUENCE + chunked($chunkSize) - processes in batches:")
        val seqResult = processAllMmfsWithSequence(chunkSize)
        printResult(seqResult)
        println("    Chunks processed: ${seqResult.chunksProcessed}")

        System.gc()
        println("\n[3] FLOW + flatMapMerge(3) - concurrent queries:")
        val flowResult = processAllMmfsWithFlowConcurrent()
        printResult(flowResult)

        println("\n" + "-".repeat(70))
        println("Summary:")
        println("  List:     ${listResult.timeMs}ms  - ALL data in memory")
        println("  Sequence: ${seqResult.timeMs}ms  - ${seqResult.chunksProcessed} chunks of $chunkSize")
        println("  Flow:     ${flowResult.timeMs}ms  - 3 queries in PARALLEL")

        val flowSpeedup = listResult.timeMs.toDouble() / flowResult.timeMs
        println("\n  Flow is %.1fx faster than List!".format(flowSpeedup))
        println("=".repeat(70))
    }

    private fun printResult(result: ProcessingResult) {
        println("    ${result.totalProcessed} customers in ${result.timeMs}ms")
        result.processedByMmf.forEach { (mmf, count) ->
            println("    - ${mmf.displayName}: $count")
        }
    }

    private fun processCustomer(customer: Customer): ProcessedOrder {
        return ProcessedOrder(
            customerId = customer.id,
            mmf = customer.mmf,
            orderType = if (customer.id % 2 == 0) OrderType.BUY else OrderType.SELL,
            amount = customer.id * 100.0
        )
    }

    private fun printMemory(label: String) {
        val runtime = Runtime.getRuntime()
        val used = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
        println("  [$label] Memory: ~${used}MB")
    }
}

data class ProcessingResult(
    val approach: String,
    val totalProcessed: Int,
    val processedByMmf: Map<MMF, Int>,
    val timeMs: Long,
    val chunksProcessed: Int = 0
)
