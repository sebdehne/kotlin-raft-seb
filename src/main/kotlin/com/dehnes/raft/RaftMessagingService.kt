package com.dehnes.raft

import java.util.*

interface RaftMessagingService {
    fun sendRequest(request: RaftRequest)
    fun reply(response: Response)
}

abstract class Message(
        val id: String,
        val from: String,
        val to: String
)

abstract class Request(
        id: String,
        from: String,
        to: String
) : Message(id, from, to)

abstract class Response(
        id: String,
        from: String,
        to: String
) : Message(id, from, to)

abstract class RaftRequest(
        id: String,
        from: String,
        to: String,
        val term: Long
) : Request(id, from, to)

abstract class RaftResponse(
        id: String,
        from: String,
        to: String,
        val term: Long
) : Response(id, from, to)

class VoteRequest(
        id: String,
        from: String,
        to: String,
        term: Long,
        val lastLogIndex: Long,
        val lastLogTerm: Long
) : RaftRequest(id, from, to, term) {
    fun response(currentTerm: Long, voteGranted: Boolean) = VoteResponse(
            id,
            to,
            from,
            currentTerm,
            voteGranted
    )

    override fun toString(): String {
        return "VoteRequest(from=$from, to=$to, term=$term, lastLogIndex=$lastLogIndex, lastLogTerm=$lastLogTerm)"
    }
}

class VoteResponse(
        id: String,
        from: String,
        to: String,
        term: Long,
        val voteGranted: Boolean
) : RaftResponse(id, from, to, term) {
    override fun toString(): String {
        return "VoteResponse(from=$from, to=$to, term=$term, voteGranted=$voteGranted)"
    }
}


class AppendEntriesRequest(
        id: String = UUID.randomUUID().toString(),
        from: String,
        to: String,
        term: Long,
        val prevLogIndex: Long,
        val prevLogTerm: Long,
        val entries: List<Any>,
        val leaderCommit: Long
) : RaftRequest(id, from, to, term) {
    fun response(currentTerm: Long, success: Boolean) = AppendEntriesResponse(
            id,
            to,
            from,
            currentTerm,
            success
    )

    override fun toString(): String {
        return "AppendEntriesRequest(from=$from, to=$to, term=$term, prevLogIndex=$prevLogIndex, prevLogTerm=$prevLogTerm, entries=${entries.size}, leaderCommit=$leaderCommit)"
    }
}

class AppendEntriesResponse(
        id: String,
        from: String,
        to: String,
        term: Long,
        val success: Boolean
) : RaftResponse(id, from, to, term) {
    override fun toString(): String {
        return "AppendEntriesResponse(from=$from, to=$to, term=$term, success=$success)"
    }
}

class ShutdownNode(
        id: String,
        from: String,
        to: String
) : Message(id, from, to)

class ClientCommandRequest(
        id: String,
        from: String,
        to: String,
        val value: Any
) : Request(id, from, to) {
    override fun toString(): String {
        return "ClientCommandRequest(id=$id, from=$from, to=$from, value=$value)"
    }
}

class ClientCommandResponse(
        id: String,
        from: String,
        to: String
) : Response(id, from, to) {
    override fun toString(): String {
        return "ClientCommandResponse(id=$id, from=$from, to=$from)"
    }
}

class GetStateSnapshotRequest(
        id: String,
        from: String,
        to: String
) : Request(id, from, to) {
    override fun toString(): String {
        return "GetStateSnapshotRequest(id=$id, from=$from, to=$to)"
    }
}

class GetStateSnapshotResponse(
        id: String,
        from: String,
        to: String,
        val term: Long,
        val state: RaftState,
        val commitIndex: Long,
        val lastApplied: Long,
        val logSize: Long
) : Response(id, from, to) {
    override fun toString(): String {
        return "GetStateSnapshotResponse(" +
                "id=$id, " +
                "from=$from, " +
                "to=$to, " +
                "term=$term, " +
                "state=$state, " +
                "commitIndex=$commitIndex, " +
                "lastApplied=$lastApplied, " +
                "logSize=$logSize" +
                ")"
    }
}

class ClusterConfigurationUpdate(
        val newConfiguration: List<String>
)
