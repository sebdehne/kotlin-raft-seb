package com.dehnes.raft

import com.dehnes.raft.RaftBase.TimerType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BasicRaftTest : RaftTestBase() {

    @Test
    fun test() {

        addCluster(mapOf(
                node1 to 250,
                node2 to 450,
                node3 to 650))

        // allow the timer to fire for node1
        allowTimers(node1, TimerType.election)
        // allow the VoteRequest to be handled
        allow2Way<VoteRequest, VoteResponse>(node1, node2)
        allow2Way<VoteRequest, VoteResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        // wait here while node1 is becoming leader
        allowExecutionAndWaitUntilAllFiltersConsumed()

        // send a value to the cluster
        sendMsg(node1, ClientCommandRequest(
                "id",
                "client",
                node1,
                "Hello World!"
        ))
        filterInbound(node1) {any -> any is ClientCommandRequest }
        // allow AppendEntriesRequest to be handled
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        // wait here until the value is replicated to all
        allowExecutionAndWaitUntilAllFiltersConsumed()

        // value should now be commited at the leader - but not yet at the followers
        assertEquals(1, fsms[node1]!!.size)
        assertEquals(0, fsms[node2]!!.size)
        assertEquals(0, fsms[node3]!!.size)

        // the heartbeat timer should handle that the value gets commited at the followers as well
        allowTimers(node1, TimerType.heartbeat)
        // allow AppendEntriesRequest to be handled
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        // wait here until the value is commited at all nodes
        allowExecutionAndWaitUntilAllFiltersConsumed()

        assertEquals(1, fsms[node1]!!.size)
        assertEquals(1, fsms[node2]!!.size)
        assertEquals(1, fsms[node3]!!.size)

    }

}