package com.dehnes.raft

import com.dehnes.raft.LogCompareResult.*
import com.dehnes.raft.persistence.InMemoryRaftStatePersistence
import com.dehnes.raft.persistence.RaftStatePersistence
import com.dehnes.raft.persistence.RaftStatePersistence.LogEntry
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class RaftNodeCompareLogTest {

    @Test
    fun localIsBetter1() {
        val result = compareLog(
                start(listOf(
                        LogEntry(1, "A"),
                        LogEntry(2, "B"),
                        LogEntry(3, "C")
                )),
                request(2, 1, 3)
        )

        assertEquals(localIsMoreUp2Date, result)
    }

    @Test
    fun localIsBetter2() {
        val result = compareLog(
                start(listOf(
                        LogEntry(1, "A"),
                        LogEntry(2, "B"),
                        LogEntry(3, "C")
                )),
                request(2, 1, 2)
        )

        assertEquals(localIsMoreUp2Date, result)
    }

    @Test
    fun remoteIsBetter1() {
        val result = compareLog(
                start(listOf(
                        LogEntry(1, "A")
                )),
                request(2, 2, 3)
        )

        assertEquals(remoteIsMoreUp2Date, result)
    }

    @Test
    fun remoteIsBetter2() {
        val result = compareLog(
                start(listOf(
                        LogEntry(1, "A")
                )),
                request(2, 2, 1)
        )

        assertEquals(remoteIsMoreUp2Date, result)
    }

    @Test
    fun areEquals() {
        val result = compareLog(
                start(listOf(
                        LogEntry(1, "A"),
                        LogEntry(2, "B")
                )),
                request(2, 1, 2)
        )

        assertEquals(equals, result)
    }

    private fun request(term: Long, lastLogIndex: Long, lastLogTerm: Long) = VoteRequest(
            "id",
            "from",
            "to",
            term,
            lastLogIndex,
            lastLogTerm
    )

    private fun start(l: List<LogEntry>): RaftStatePersistence {
        val inMemoryRaftStatePersistence = InMemoryRaftStatePersistence(emptyList())
        l.forEach { e -> inMemoryRaftStatePersistence.appendLog(e) }
        return inMemoryRaftStatePersistence
    }
}