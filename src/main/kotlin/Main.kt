package org.danikotlingdemos

import kotlinx.coroutines.runBlocking
import org.danikotlingdemos.listSequenceFlow.DataProcessor

fun main() = runBlocking {
    val processor = DataProcessor()
    processor.runDemo()
}
