package com.dehnes.raft

import com.dehnes.raft.persistence.RaftStatePersistence
import mu.KLogger
import kotlin.math.min


fun findMajorityMatchIndex(
        matchedIndex: Map<String, Long>
): Long {
    var candidate = matchedIndex.values.max()!!
    var candidateMatches = 0
    while (true) {
        for (node in matchedIndex.keys) {
            val nodeIndex = matchedIndex[node]!!
            if (nodeIndex >= candidate) {
                candidateMatches++
            }
        }
        if (candidateMatches * 2 > matchedIndex.size) {
            return candidate
        }
        candidateMatches = 0
        candidate--
    }
}

internal fun handleLog(
        raftStatePersistence: RaftStatePersistence,
        request: AppendEntriesRequest,
        commitIndex: Long,
        logger: KLogger,
        setCommitIndex: (i: Long) -> Unit
): Boolean {
    val lastIndex = raftStatePersistence.getLogSize() - 1
    if (request.prevLogIndex > lastIndex) {
        // entries missing - cannot proceed
        return false
    }

    val prevLogEntryTerm = if (request.prevLogIndex >= 0) raftStatePersistence.getLogAt(request.prevLogIndex).term else 0
    if (prevLogEntryTerm != request.prevLogTerm) {
        // mismatch, remove all stale entries
        raftStatePersistence.removeAllFromIndex(request.prevLogIndex)
        return false
    }

    var index = request.prevLogIndex + 1
    request.entries.forEach { entry ->
        val logEntry = RaftStatePersistence.LogEntry(request.term, entry)
        if (index < raftStatePersistence.getLogSize()) {
            raftStatePersistence.setLogAt(index, logEntry)
        } else {
            raftStatePersistence.appendLog(logEntry)
            if (logEntry.entry is RaftStatePersistence.ClusterConfiguration) {
                logger.info { "Applied new config ${logEntry.entry}" }
            }
        }
        index++
    }

    if (request.leaderCommit > commitIndex) {
        setCommitIndex(min(request.leaderCommit, raftStatePersistence.getLogSize() - 1))
    }

    return true
}

fun hasMajorityOfVotes(gotVotes: List<String>, otherNodes: List<String>): Boolean {
    return gotVotes.size * 2 > (otherNodes.size + 1)
}

fun compareLog(
        raftStatePersistence: RaftStatePersistence,
        voteRequest: VoteRequest): LogCompareResult {
    val localLastIndex = raftStatePersistence.getLogSize() - 1
    val localLastTerm = if (localLastIndex >= 0) raftStatePersistence.getLogAt(localLastIndex).term else 0

    val termCompare = localLastTerm.compareTo(voteRequest.lastLogTerm)
    return LogCompareResult.fromIntValue(if (termCompare != 0) {
        termCompare
    } else {
        localLastIndex.compareTo(voteRequest.lastLogIndex)
    })
}

enum class LogCompareResult {
    equals,
    localIsMoreUp2Date,
    remoteIsMoreUp2Date;

    companion object {
        fun fromIntValue(compareValue: Int) = when {
            compareValue > 0 -> localIsMoreUp2Date
            compareValue == 0 -> equals
            compareValue < 0 -> remoteIsMoreUp2Date
            else -> throw RuntimeException("Impossible")
        }
    }
}