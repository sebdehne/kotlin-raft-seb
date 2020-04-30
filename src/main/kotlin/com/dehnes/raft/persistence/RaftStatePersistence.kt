package com.dehnes.raft.persistence

interface RaftStatePersistence {

    fun getCurrentTerm(): Long // init 1
    fun setCurrentTerm(term: Long)

    fun getVotedFor(): String? // init null
    fun setVotedFor(votedFor: String)
    fun clearVotedFor()

    fun getLogSize(): Long
    fun getLogAt(index: Long): LogEntry
    fun setLogAt(index: Long, logEntry: LogEntry)
    fun appendLog(logEntry: LogEntry) // must also update ClusterConfiguration atomically
    fun logSubList(fromIndex: Long, toExcludingIndex: Long): List<LogEntry>
    fun removeAllFromIndex(fromIndex: Long)

    fun getGotVotes(): List<String>
    fun appendGotVote(followerId: String)
    fun clearGotVotes()

    fun getClusterConfiguration(): ClusterConfiguration

    data class LogEntry(
            val term: Long,
            val entry: Any
    )

    data class ClusterConfiguration(
            val cluster: List<String>,
            val transitioningTo: List<String>?
    ) {
        fun isInTransition() = transitioningTo != null
        fun startTransition(newCluster: List<String>) = ClusterConfiguration((this.cluster + newCluster).distinct(), newCluster)
        fun endTransition() = ClusterConfiguration(transitioningTo!!, null)
    }
}