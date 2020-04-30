package com.dehnes.raft

import com.dehnes.raft.RaftBase.TimerType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class VotingRaftTest : RaftTestBase() {

    @Test
    fun splitVote() {

        addCluster(mapOf(
                node1 to 200,
                node2 to 200,
                node3 to 200))

        // allow the timer to fire for node1
        allowTimers(node1, TimerType.election)
        allowTimers(node2, TimerType.election)
        allowTimers(node3, TimerType.election)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        // allow all VoteRequests/VoteResponses to be exchanged
        allow2Way<VoteRequest, VoteResponse>(node1, node2)
        allow2Way<VoteRequest, VoteResponse>(node1, node3)
        allow2Way<VoteRequest, VoteResponse>(node2, node1)
        allow2Way<VoteRequest, VoteResponse>(node2, node3)
        allow2Way<VoteRequest, VoteResponse>(node3, node1)
        allow2Way<VoteRequest, VoteResponse>(node3, node2)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        assertEquals(RaftState.candidate, getState(node1).state)
        assertEquals(RaftState.candidate, getState(node2).state)
        assertEquals(RaftState.candidate, getState(node3).state)
    }

    @Test
    fun oneNodeIsolated() {

        addCluster(mapOf(
                node1 to 150,
                node2 to 250,
                node3 to 350)
        )

        // node1 would win the election, but we are going to isolate it, so then node2 wins
        allowTimers(node1, TimerType.election)
        drop<VoteRequest>(node1, node2)
        drop<VoteRequest>(node1, node3)

        allowTimers(node2, TimerType.election)
        drop<VoteRequest>(node2, node1)
        allow2Way<VoteRequest, VoteResponse>(node2, node3)
        drop<AppendEntriesRequest>(node2, node1)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node2, node3)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        getState("node1").let {
            assertEquals(RaftState.candidate, it.state)
            assertEquals(2, it.term)
        }
        getState("node2").let {
            assertEquals(RaftState.leader, it.state)
            assertEquals(2, it.term)
        }
        getState("node3").let {
            assertEquals(RaftState.following, it.state)
            assertEquals(2, it.term)
        }

        // now node1 is allowed to communicate and becomes the leader
        allowTimers(node1, TimerType.election)
        allow2Way<VoteRequest, VoteResponse>(node1, node2)
        allow2Way<VoteRequest, VoteResponse>(node1, node3)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node2)
        allow2Way<AppendEntriesRequest, AppendEntriesResponse>(node1, node3)

        allowExecutionAndWaitUntilAllFiltersConsumed()

        getState("node1").let {
            assertEquals(RaftState.leader, it.state)
            assertEquals(3, it.term)
        }
        getState("node2").let {
            assertEquals(RaftState.following, it.state)
            assertEquals(3, it.term)
        }
        getState("node3").let {
            assertEquals(RaftState.following, it.state)
            assertEquals(3, it.term)
        }

    }

}