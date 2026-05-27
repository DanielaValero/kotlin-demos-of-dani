package org.danikotlingdemos.listSequenceFlow

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Simulates a database for MMF (Money Market Fund) processing.
 *
 * Demonstrates three fetching approaches:
 * - List: Eager, blocking (Thread.sleep)
 * - Sequence: Lazy, blocking (Thread.sleep)
 * - Flow: Async, non-blocking (delay) - enables parallelization
 */
class FakeDatabase(
    private val totalCustomers: Int = 100_000,
    private val queryDelayMs: Long = 200L  // Simulates real DB latency
) {
    private fun generateCustomersForMmf(mmf: MMF): List<Customer> {
        return (1..totalCustomers)
            .filter { MMF.entries[it % MMF.entries.size] == mmf }
            .map { id ->
                Customer(
                    id = id,
                    coreAccountNumber = "ACC-${id.toString().padStart(8, '0')}",
                    settlementEscrow = "ESC-${(id % 100).toString().padStart(4, '0')}",
                    mmf = mmf,
                    isEligible = id % 5 != 0  // ~80% eligible
                )
            }
    }

    /**
     * LIST - Eager, blocking.
     * Uses Thread.sleep which BLOCKS the thread.
     */
    fun fetchCustomersByMmfAsList(mmf: MMF): List<Customer> {
        Thread.sleep(queryDelayMs)
        return generateCustomersForMmf(mmf)
    }

    /**
     * SEQUENCE - Lazy, blocking.
     * Still uses Thread.sleep (blocking), but processes one item at a time.
     */
    fun fetchCustomersByMmfAsSequence(mmf: MMF): Sequence<Customer> = sequence {
        Thread.sleep(queryDelayMs)
        generateCustomersForMmf(mmf).forEach { yield(it) }
    }

    /**
     * FLOW - Async, non-blocking.
     * Uses delay() which SUSPENDS (non-blocking).
     * This allows flatMapMerge to run multiple queries IN PARALLEL!
     */
    fun fetchCustomersByMmfAsFlow(mmf: MMF): Flow<Customer> = flow {
        delay(queryDelayMs)
        generateCustomersForMmf(mmf).forEach { emit(it) }
    }

    fun getStatistics(): DatabaseStats {
        val allCustomers = MMF.entries.flatMap { generateCustomersForMmf(it) }
        return DatabaseStats(
            totalCustomers = allCustomers.size,
            eligibleCustomers = allCustomers.count { it.isEligible },
            customersByMmf = allCustomers.groupBy { it.mmf }.mapValues { it.value.size }
        )
    }
}

data class Customer(
    val id: Int,
    val coreAccountNumber: String,
    val settlementEscrow: String,
    val mmf: MMF,
    val isEligible: Boolean,
    val payload: String = "x".repeat(1000)  // ~1KB per object
)

enum class MMF(val displayName: String) {
    AMUNDI("Amundi"),
    DWS("DWS"),
    BLACKROCK("Blackrock")
}

data class ProcessedOrder(
    val customerId: Int,
    val mmf: MMF,
    val orderType: OrderType,
    val amount: Double
)

enum class OrderType { BUY, SELL }

data class DatabaseStats(
    val totalCustomers: Int,
    val eligibleCustomers: Int,
    val customersByMmf: Map<MMF, Int>
)
