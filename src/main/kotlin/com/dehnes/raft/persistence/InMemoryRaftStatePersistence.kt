package com.dehnes.raft.persistence

import com.dehnes.raft.persistence.RaftStatePersistence.LogEntry

class InMemoryRaftStatePersistence(
        initialCluster: List<String>
) : RaftStatePersistence {

    private var currentTerm = 1L
    private var votedFor: String? = null
    private val log = mutableListOf<LogEntry>()
    private val gotVotes = mutableListOf<String>()
    private var cluster = RaftStatePersistence.ClusterConfiguration(initialCluster, null)

    override fun getCurrentTerm() = currentTerm

    override fun setCurrentTerm(term: Long) {
        currentTerm = term
    }

    override fun getVotedFor() = votedFor

    override fun setVotedFor(votedFor: String) {
        this.votedFor = votedFor
    }

    override fun clearVotedFor() {
        votedFor = null
    }

    override fun getLogSize() = log.size.toLong()

    override fun getLogAt(index: Long) = log[index.toInt()]

    override fun setLogAt(index: Long, logEntry: LogEntry) {
        log[index.toInt()] = logEntry
    }

    override fun appendLog(logEntry: LogEntry) {
        log.add(logEntry)
        if (logEntry.entry is RaftStatePersistence.ClusterConfiguration) {
            cluster = logEntry.entry
        }
    }

    override fun logSubList(fromIndex: Long, toExcludingIndex: Long) = log.subList(
            fromIndex.toInt(),
            toExcludingIndex.toInt()
    )

    override fun removeAllFromIndex(fromIndex: Long) {
        while (log.size > fromIndex) log.removeAt(log.size - 1)
    }

    override fun getGotVotes() = gotVotes.toList()

    override fun appendGotVote(followerId: String) {
        gotVotes.add(followerId)
    }

    override fun clearGotVotes() {
        gotVotes.clear()
    }

    override fun getClusterConfiguration() = cluster

}