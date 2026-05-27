package org.danikotlingdemos

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Simulates a database that stores customer data for MMF (Money Market Fund) processing.
 *
 * Based on the presentation "Scaling Data Processing in Kotlin without Melting Your Memory",
 * this class demonstrates three approaches to fetching data:
 * - List (eager): Loads all data at once - high memory usage
 * - Sequence (lazy): Streams data one item at a time - memory efficient
 * - Flow (async): Streams data with suspend support - enables concurrent processing
 */
class FakeDatabase(
    private val totalCustomers: Int = 20_000,
    private val fetchDelayMs: Long = 1L
) {
    // Simulates the database "table" of customers
    private val customers: List<Customer> by lazy {
        (1..totalCustomers).map { id ->
            Customer(
                id = id,
                coreAccountNumber = "ACC-${id.toString().padStart(8, '0')}",
                settlementEscrow = "ESC-${(id % 100).toString().padStart(4, '0')}",
                mmf = MMF.entries[id % MMF.entries.size],
                isEligible = id % 3 != 0  // ~66% are eligible
            )
        }
    }

    // =========================================================================
    // LIST - Eager fetch: Loads ALL matching data into memory at once
    // =========================================================================
    /**
     * Fetches all customers for a given MMF as a List.
     * WARNING: This loads ALL data into memory at once!
     *
     * Equivalent to JOOQ: `.fetch()` which returns a Java List
     */
    fun fetchCustomersByMmfAsList(mmf: MMF): List<Customer> {
        Thread.sleep(fetchDelayMs * 10) // Simulate query execution time
        return customers.filter { it.mmf == mmf }
    }

    /**
     * Fetches all eligible customers as a List.
     * WARNING: With 100k+ customers, this consumes significant memory!
     */
    fun fetchEligibleCustomersAsList(): List<Customer> {
        Thread.sleep(fetchDelayMs * 10)
        return customers.filter { it.isEligible }
    }

    // =========================================================================
    // SEQUENCE - Lazy fetch: Streams data one item at a time
    // =========================================================================
    /**
     * Fetches customers for a given MMF as a Sequence (lazy).
     *
     * Equivalent to JOOQ:
     * ```kotlin
     * .fetchSize(fetchSize)
     * .fetchStream()
     * .asSequence()
     * ```
     *
     * The Sequence processes one item at a time through the entire pipeline,
     * drastically reducing memory usage for large datasets.
     */
    fun fetchCustomersByMmfAsSequence(mmf: MMF, fetchSize: Int = 1000): Sequence<Customer> {
        return sequence {
            var fetched = 0
            customers
                .filter { it.mmf == mmf }
                .chunked(fetchSize)
                .forEach { chunk ->
                    // Simulates fetching a batch from the DB cursor
                    Thread.sleep(fetchDelayMs)
                    chunk.forEach { customer ->
                        fetched++
                        yield(customer)
                    }
                }
        }
    }

    /**
     * Fetches all eligible customers as a Sequence with chunked fetching.
     * Memory-efficient for processing millions of rows.
     */
    fun fetchEligibleCustomersAsSequence(fetchSize: Int = 1000): Sequence<Customer> {
        return sequence {
            customers
                .filter { it.isEligible }
                .chunked(fetchSize)
                .forEach { chunk ->
                    Thread.sleep(fetchDelayMs)
                    chunk.forEach { yield(it) }
                }
        }
    }

    // =========================================================================
    // FLOW - Async fetch: Streams with suspend support for concurrent processing
    // =========================================================================
    /**
     * Fetches customers for a given MMF as a Flow (async, cold stream).
     *
     * The Flow builder enables:
     * - Non-blocking I/O (suspends during fetch, freeing the thread)
     * - Concurrent processing with flatMapMerge
     * - Backpressure handling
     *
     * Simulates the pattern from the presentation:
     * ```kotlin
     * fun readLargeEligibleDataset(mmf: String): Flow<Customer> = flow {
     *     val lazyQuery = dbContext.selectFrom(CUSTOMERS)
     *         .where(CUSTOMERS.MMF.eq(mmf))
     *         .fetchLazy()
     *         .fetchSize(100)
     *     try {
     *         for (row in lazyQuery) {
     *             emit(row.toCustomerDomainModel())
     *         }
     *     } finally {
     *         lazyQuery.close()
     *     }
     * }
     * ```
     */
    fun fetchCustomersByMmfAsFlow(mmf: MMF, fetchSize: Int = 1000): Flow<Customer> = flow {
        customers
            .filter { it.mmf == mmf }
            .chunked(fetchSize)
            .forEach { chunk ->
                delay(fetchDelayMs) // Non-blocking suspend
                chunk.forEach { emit(it) }
            }
    }

    /**
     * Fetches all eligible customers as a Flow.
     * Perfect for concurrent processing with flatMapMerge.
     */
    fun fetchEligibleCustomersAsFlow(fetchSize: Int = 1000): Flow<Customer> = flow {
        customers
            .filter { it.isEligible }
            .chunked(fetchSize)
            .forEach { chunk ->
                delay(fetchDelayMs)
                chunk.forEach { emit(it) }
            }
    }

    /**
     * Writes a batch of processed results to the database.
     * Simulates the `persistOrderBatch()` operation from the presentation.
     */
    suspend fun writeResults(results: List<ProcessedOrder>) {
        delay(fetchDelayMs)
        // In real code, this would batch insert to DB
    }

    /**
     * Writes a single result to the database.
     */
    suspend fun writeResult(result: ProcessedOrder) {
        delay(fetchDelayMs / 10)
    }

    /**
     * Returns statistics about the data distribution.
     */
    fun getStatistics(): DatabaseStats {
        val byMmf = customers.groupBy { it.mmf }
        return DatabaseStats(
            totalCustomers = totalCustomers,
            eligibleCustomers = customers.count { it.isEligible },
            customersByMmf = byMmf.mapValues { it.value.size }
        )
    }
}

// =============================================================================
// Domain Models
// =============================================================================

/**
 * Customer entity based on the presentation's domain model.
 */
data class Customer(
    val id: Int,
    val coreAccountNumber: String,
    val settlementEscrow: String,
    val mmf: MMF,
    val isEligible: Boolean
)

/**
 * Money Market Funds from the presentation example.
 */
enum class MMF(val displayName: String, val dataPercentage: Int) {
    AMUNDI("Amundi", 10),
    DWS("DWS", 40),
    BLACKROCK("Blackrock", 50)
}

/**
 * Processed order result after applying business logic.
 */
data class ProcessedOrder(
    val customerId: Int,
    val mmf: MMF,
    val orderType: OrderType,
    val amount: Double
)

enum class OrderType {
    BUY, SELL, BALANCE
}

data class DatabaseStats(
    val totalCustomers: Int,
    val eligibleCustomers: Int,
    val customersByMmf: Map<MMF, Int>
)
