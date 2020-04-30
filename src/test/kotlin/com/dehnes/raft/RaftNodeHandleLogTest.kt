package com.dehnes.raft

import com.dehnes.raft.persistence.RaftStatePersistence
import com.dehnes.raft.persistence.RaftStatePersistence.LogEntry
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class RaftNodeHandleLogTest {
    val logger = KotlinLogging.logger { }

    @Test
    fun emptyLog() {
        // given
        val logStart = listOf<LogEntry>()
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, -1, 0, 0),
                0,
                logger,
                {}
        )
        assertEquals(true, success)
        assertEquals(logStart, log)
    }

    @Test
    fun oneEntry() {
        // given
        val logStart = listOf<LogEntry>()
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, -1, 0, 0, "A"),
                0,
                logger,
                {}
        )
        assertEquals(true, success)
        assertEquals(listOf(LogEntry(2, "A")), log)
    }

    @Test
    fun appendOnEmptyOK() {
        val logStart = listOf<LogEntry>()
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, -1, 0, 0, "A", "B"),
                0,
                logger,
                {}
        )
        assertEquals(true, success)
        assertEquals(listOf(LogEntry(2, "A"), LogEntry(2, "B")), log)
    }

    @Test
    fun appendOK() {
        val logStart = listOf(LogEntry(2, "A"), LogEntry(2, "B"))
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, 1, 2, 0, "C", "D"),
                0,
                logger,
                {}
        )
        assertEquals(true, success)
        assertEquals(listOf(
                LogEntry(2, "A"),
                LogEntry(2, "B"),
                LogEntry(2, "C"),
                LogEntry(2, "D")
        ), log)
    }

    @Test
    fun doNotHavePrevIndex() {
        val logStart = listOf(LogEntry(2, "A"))
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, 1, 2, 0, "C", "D"),
                0,
                logger,
                {}
        )
        assertEquals(false, success)
        assertEquals(listOf(LogEntry(2, "A")), log)
    }

    @Test
    fun doNotHavePrevTerm() {
        val logStart = listOf(LogEntry(1, "Z"))
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, 0, 2, 0, "B", "C"),
                0,
                logger,
                {}
        )
        assertEquals(false, success)
        assertEquals(listOf<LogEntry>(), log)
    }

    @Test
    fun doNotHavePrevTerm2() {
        val logStart = listOf(
                LogEntry(1, "A"),
                LogEntry(1, "B"),
                LogEntry(1, "C")
        )
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, 1, 2, 0, "B1", "C1"),
                0,
                logger,
                {}
        )
        assertEquals(false, success)
        assertEquals(listOf(LogEntry(1, "A")), log)
    }

    @Test
    fun replaceAndAppend() {
        val logStart = listOf(
                LogEntry(1, "A"),
                LogEntry(1, "B"),
                LogEntry(1, "C")
        )
        val log = logStart.toMutableList()
        val success = handleLog(
                logService(log),
                request(2, 1, 1, 0, "C1", "D1"),
                0,
                logger,
                {}
        )
        assertEquals(true, success)
        assertEquals(listOf(
                LogEntry(1, "A"),
                LogEntry(1, "B"),
                LogEntry(2, "C1"),
                LogEntry(2, "D1")
        ), log)
    }

    private fun request(term: Long, prevLogIndex: Long, prevLogTerm: Long, leaderCommit: Long, vararg entries: String) = AppendEntriesRequest(
            "id",
            "from",
            "to",
            term,
            prevLogIndex,
            prevLogTerm,
            entries.toList(),
            leaderCommit
    )

    private fun logService(log: MutableList<LogEntry>) = object : RaftStatePersistence {

        override fun getLogSize() = log.size.toLong()

        override fun getLogAt(index: Long) = log[index.toInt()]

        override fun setLogAt(index: Long, logEntry: LogEntry) {
            log[index.toInt()] = logEntry
        }

        override fun appendLog(logEntry: LogEntry) {
            log.add(logEntry)
        }

        override fun logSubList(fromIndex: Long, toExcludingIndex: Long): List<LogEntry> {
            return log.subList(fromIndex.toInt(), toExcludingIndex.toInt())
        }

        override fun removeAllFromIndex(fromIndex: Long) {
            while (log.size > fromIndex) log.removeAt(log.size - 1)
        }

        override fun getCurrentTerm(): Long {
            TODO("Not yet implemented")
        }

        override fun setCurrentTerm(term: Long) {
            TODO("Not yet implemented")
        }

        override fun getVotedFor(): String? {
            TODO("Not yet implemented")
        }

        override fun setVotedFor(votedFor: String) {
            TODO("Not yet implemented")
        }

        override fun clearVotedFor() {
            TODO("Not yet implemented")
        }

        override fun getGotVotes(): List<String> {
            TODO("Not yet implemented")
        }

        override fun appendGotVote(followerId: String) {
            TODO("Not yet implemented")
        }

        override fun clearGotVotes() {
            TODO("Not yet implemented")
        }

        override fun getClusterConfiguration(): RaftStatePersistence.ClusterConfiguration {
            TODO("Not yet implemented")
        }

    }
}