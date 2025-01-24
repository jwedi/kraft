
Basic Rust implementation of the Raft consensus algorithm.


TODOs
- Ping gRPC server running
- Server running in Docker
- Leader election
  - No-op AppendEntries heartbeats
- Persist state to disk. i.e Leader for current term
- Log replication



- Circle buffer with job commands.
- Sync writer, single reader.
- Writers get index of buffer they can write to. (Sync increment index, cannot overrun the read index)
- Reader smart batching.
- Timer every N ms check if buffer is empty, if true, add hearbeat command.
- Queueing commands return some sort of future value that is written to when the reader has processed the command and the writer can read from / await.



Overall flow:
Leader election:
- 
Writes:
- Client connects to one of the servers.
- Client sends insert request to server.
- If server is the master it adds the insert command into the command queue and waits for a response.
  - The command worker reads commands into a smart batch.
  - It first writes a log entry to the local persistent log.
  - It then sends an AppendEntries RPC to all other servers to replicate the log entry.
  - If a majority of servers respond with success, the leader commits the log entry.
  - The next AppendEntries request from the leader to the followers will now contain the new commit index.
  - The leader then updates its state machine and responds to the client.
- If server is not the master it forwards the request to the master.
  - The follower waits for a positive response from the leader and then returns with the result.
- On response on channel it returns the response to the client.
Reads:
- The server sends a read command to its local job queue.
- The worker reads the commands and completes the request by reading from the state machine.
- The worker then responds with the result to the response channel.
- The server then returns the response to the client.
- Something with commit index to not serve reads that haven't been replicated to a majority yet.


AppendEntries:
- Potentially unique payload per follower due to backfilling entries.
- Leader prepares batch of entries from follower.matchIndex -> leader.currentIndex.
- It sends AppendEntries to given follower
  - with prevIndex and prevTerm taken from first entry in batch.
  - with lastCommitted taken from leader.commitIndex
  - with leaderId taken from leader config.
  - with term taken from leader.currentTerm.


RequestVote:
- Same payload for all followers.
- It sends RequestVote to each follower:
  - term being the new term, i.e old term +1.
  - index of last entry applied
  - term of last entry applied.
  - candidate id taken from server config.




Performance thoughts.
- Should smart batch on each follower node before forwarding writes to leader
  - Reduces networking overhead
  - Achieves constant load
- Should smart batch on leader worker
  - Reduces Bookeeping overhead
  - Reduces IO overhead by batching writes together
- Maybe some CAS thingy to avoid blocking the main working thread.
  - API issues a request like PrepareVote, 
  - the worker would validate the request and reserve an IO "Mutex" to the caller where they can write the data themselves, 
  - then the caller would send something like CommitVote and the worker would then consider the vote accepted.
  - (I may have re-invented Futures here)
  - A follower can only respond positively to a RequestVote after it has persisted its vote to disk, this will block the main thread unless i do something about it.
- Is blocking the main thread for AppendEntries requests actually an issue?
  - Writes have to be synchronised to disk, which makes blocking writes a good thing rather than a bad thing.
  - The next disk write could probably be prepared while waiting for the disk write to complete.
  - Like some kind of pipelining strategy where the system can have one pipelining and one internal processing task active at a time.
  - If there's no stable leader, then i would not expect AppendEntries and RequestVote requests to happen in parallell, maybe that's mostly a non-issue.
- As long as the IO cost is amortized over a large enough number of requests, I should be fine.
  - Maybe each request to the executor should contain both the request for validation but also the serialized version of the data that can immediately be dumped into the write buffer
  - The worker would have a fixed size write buffer and smart batches more requests until the batch size is too large for the buffer.
  - The buffer is reset after each IO dump, alternatively a new epoc buffer is created.
  - So smart batch incoming requests while there are more on the queue, no leader election is happening and write buffer allows more data.
- A batch write involves both a write to disk and then a RPC to all followers to accept the request.
  - Doing all of this blocking IO on the main thread sounds terrible.
  - Maybe one NodeHandler per node in the cluster. Is also a State machine, where it continously polls heartbeats while in Leader state.






More TODOs
- API needs to send message to worker to get the current leader id for writes.
  - Should locally cache response for a short period of time.
  - Writes should only go to leader.
  - 
- API needs to put write requests onto internal queue and they need to be smart batched.
- Whenever writes have been written to a quorum majority
  - Internal datastructure like btree should be updated 
  - callback should fire to respond to API client
  - Some type of message should be sent to learner
- Maybe during smart batch build one message containing all writes in the batch. 
- Insert pending message with payload that's all callbacks, on response respond to all callbacks.

Having separate works queues for append entries and control messages would be convenient.
Even if i did, i would still need to peek messages and potentially not take them.
Maybe add a simple array buffer in the queue worker that's processed before the queue and where messages can be stored for later.

Event loop {
  pop message from array buffer.
  if control message execute immediately
  if append message, keep polling from array buffer until batch is full.
  Maybe make a wrapper struct that wraps the SegQueue adds, peek, and nextAppendRequest.
}


Read flow
1. Read request is sent to the internal read proxy (?)
2. Read proxy reads data from internal datastructure (?)
   3. If that's the case then how is the datastructure updated?
   4. We don't want lock contention for each request, maybe the same single worker approach(?)

Write flow
1. Write request is sent to write proxy
2. Write proxy figures out who the leader is
3. Write proxy batches requests into a smart batch and then forwards to leader.
4. (Leader can't be having the combined RPS of all nodes)
5. Would maybe not need to have leader level batching if that is the case.
6. 