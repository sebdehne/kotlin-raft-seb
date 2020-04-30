package com.dehnes.raft

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RaftNodeFindMajorityMatchedIndexTest {

    @Test
    fun test5() {
        val matchedIndex = mapOf(
                "A" to 10L,
                "B" to 9L,
                "C" to 8L,
                "D" to 7L,
                "E" to 6L
        )

        assertEquals(8, findMajorityMatchIndex(matchedIndex))
    }

    @Test
    fun test4() {
        val matchedIndex = mapOf(
                "A" to 10L,
                "B" to 9L,
                "C" to 8L,
                "D" to 7L
        )

        assertEquals(8, findMajorityMatchIndex(matchedIndex))
    }

    @Test
    fun test3() {
        val matchedIndex = mapOf(
                "A" to 10L,
                "B" to 9L,
                "C" to 8L
        )

        assertEquals(9, findMajorityMatchIndex(matchedIndex))
    }
}