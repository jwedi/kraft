
Basic Rust implementation of the Raft consensus algorithm.


TODOs
- Ping gRPC server running, Done
- Server running in Docker
- Leader election, Done
  - No-op AppendEntries heartbeats, Done
- Persist state to disk. i.e Leader for current term, Done
- Log replication, Done



- Circle buffer with job commands, Done using Crossbeam
- Sync writer, single reader, Done
- Writers get index of buffer they can write to. (Sync increment index, cannot overrun the read index), N/A due to crossbeam segqueue
- Reader smart batching, Done
- Timer every N ms check if buffer is empty, if true, add hearbeat command, Done
- Queueing commands return some sort of future value that is written to when the reader has processed the command and the writer can read from / await. Done using oneshot queues for callbacks.



Overall flow:
Leader election:
- 
Writes:
- Client connects to one of the servers. Done
- Client sends insert request to server. Done
- If server is the master it adds the insert command into the command queue and waits for a response. Done
  - The command worker reads commands into a smart batch. Done
  - It first writes a log entry to the local persistent log. Done
  - It then sends an AppendEntries RPC to all other servers to replicate the log entry. Done
  - If a majority of servers respond with success, the leader commits the log entry. Done
  - The next AppendEntries request from the leader to the followers will now contain the new commit index. TODO
  - The leader then updates its state machine and responds to the client. TODO as in needs to update soem in-memory data structure that's used for reads. TODO
- If server is not the master it forwards the request to the master. Done
  - The follower waits for a positive response from the leader and then returns with the result. Done
- On response on channel it returns the response to the client. Done
Reads: TODO
- The server sends a read command to its local job queue.
- The worker reads the commands and completes the request by reading from the state machine.
- The worker then responds with the result to the response channel.
- The server then returns the response to the client.
- Something with commit index to not serve reads that haven't been replicated to a majority yet.


AppendEntries:
- Potentially unique payload per follower due to backfilling entries.
- Leader prepares batch of entries from follower.matchIndex -> leader.currentIndex. TODO, no backfilling in place and always sends next
- It sends AppendEntries to given follower
  - with prevIndex and prevTerm taken from first entry in batch. Done?
  - with lastCommitted taken from leader.commitIndex
  - with leaderId taken from leader config. Done
  - with term taken from leader.currentTerm. Done


RequestVote: Done
- Same payload for all followers. Done
- It sends RequestVote to each follower: Done
  - term being the new term, i.e old term +1. Done
  - index of last entry applied. Done?
  - term of last entry applied. Done?
  - candidate id taken from server config. Done




Performance thoughts.
- Should smart batch on each follower node before forwarding writes to leader. Done
  - Reduces networking overhead
  - Achieves constant load
- Should smart batch on leader worker. Done
  - Reduces Bookeeping overhead
  - Reduces IO overhead by batching writes together
- Maybe some CAS thingy to avoid blocking the main working thread. Effectively done using the outstanding messages bookeeping in the state machine worker.
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
- As long as the IO cost is amortized over a large enough number of requests, I should be fine. This should effectively be done.
  - Maybe each request to the executor should contain both the request for validation but also the serialized version of the data that can immediately be dumped into the write buffer
  - The worker would have a fixed size write buffer and smart batches more requests until the batch size is too large for the buffer.
  - The buffer is reset after each IO dump, alternatively a new epoc buffer is created.
  - So smart batch incoming requests while there are more on the queue, no leader election is happening and write buffer allows more data.
- A batch write involves both a write to disk and then a RPC to all followers to accept the request. Done using persistence worker and one quorum worker per other node in the cluster.
  - Doing all of this blocking IO on the main thread sounds terrible.
  - Maybe one NodeHandler per node in the cluster. Is also a State machine, where it continously polls heartbeats while in Leader state.


Backfill and copy prevention:
1. To backfill log entries the leader needs to be able to go back to an arbitrary index in the log and re-issue append entries requests for those entries.
2. Right now no copies of past append entries requests is kept, so this is non-trivial.
3. If instead of each quorum worker and persistence worker getting their own copy of the append entries data there would be a shared datastructure with all of the data. Then no copying of data would need to be done by the leader to send it to the actor. The quorum workers could also backfill on their own by reading the log. The event passing between the leader and actors would also be simpler because the meat of the data would be in the shared log and not in the messages.
4. Alternatives:
5. im:Vector. Seem to have overall good performance characteristics both for reads and writes
6. Normal Vec. Good performance characteristics as long as the vec doesn't need to get expanded. Doesn't handle chunking very well AFAIK.
7. To know which index to start backfilling from given that there are terms, would need some indexing to say which term starts where so you can get index with index of term start + index.
8. Backfill means that the restarted node tells the leader quorum worker it's last term + index, the quorum node looks up the term start index and fetches index = term_start + prev_index + 1.
9. On leader write batch. Validate, create new sequence number, append to shared log, write message to actors. Actors read payload from log.

Leader maintains a vec of all previous append entries requests or log entries.
When a follower needs to backfill it sends a message to the leader with the last term and index it has.
The leader finds the term start and calculates the index offset. It returns a slice of the log, a Vec<Arc<LogEntry>> that the follower can use to backfill.
The followers instead of immediately issuing a append entries request when receiving a message from the leader, they add it to a local work backlog and each worker iteration it proceeds with the next entry.
This way means that the worker only needs one flow for sending append entries request and doesn't get weird when backfilling is in progress.
The quorum workers should get append entries requests as Arcs from the leader



More TODOs
- API needs to send message to worker to get the current leader id for writes. Done, not sure if caching is actually needed due to handling of the request on the state machine is just a hashmap lookup.
  - Should locally cache response for a short period of time.
  - Writes should only go to leader.
  - 
- API needs to put write requests onto internal queue and they need to be smart batched. Done
- Whenever writes have been written to a quorum majority
  - Internal datastructure like btree should be updated. TODO
  - callback should fire to respond to API client. Done
  - Some type of message should be sent to learner. TODO
- Maybe during smart batch build one message containing all writes in the batch. Done
- Insert pending message with payload that's all callbacks, on response respond to all callbacks. Done

Having separate works queues for append entries and control messages would be convenient. Done
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

Write flow. Done
1. Write request is sent to write proxy
2. Write proxy figures out who the leader is
3. Write proxy batches requests into a smart batch and then forwards to leader.
4. (Leader can't be having the combined RPS of all nodes)
5. Would maybe not need to have leader level batching if that is the case.
6. Leader picks up a write batch from queue
7. It validates incoming write requests.
8. It serializes the data in a format that can be written to disk.
9. It writes the serialized data to the persistence handler and all quorum workers.
10. When data is persisted locally and on quorum of nodes it triggers the batch callback and notifies learners.


TODOs update:
1. Verify append entries prev index being sent correctly. Done
2. Respect append entries prev index in follower, i.e don't commit if append entries prev index doesn't match follower last index. Done
3. Some sort of backfilling in follower, notify leader of the services last index and term so that leader can send log entries for backfilling.
4. Read log entries from disk on bootup and bootstrap config based on persisted log stuff. Done
5. Fix log replication on restarted node. Currently starts up with previous log index being 0. Done
6. Actually serialize real data and persist to disk. Done
7. Metrics
8. Metrics exporter
9. Bidirectional stream for quorum workers. Ensures append log is delivered in-order. Maybe quorum worker try establish connections at random intervals. Done 
- Phone exchange, node with highest id wins if duplicated streams.
- Global stream manager with some locking for each bidirectional stream. IO is much more expensive than locking, especially for single writer, overhead should be negible.
10. Rename to Kraft, Done
11. Validate business logic on leader before commit
12. Redo and initiate tracing for write proxy batch.
13. Performance profiling. 
14. Emit writes to learners?
15. Send commit index in append entries after persisted on quorum of nodes.
16. Implement query data structure for reads such as btree.
17. Don't copy/allocate serialized data for persistence worker and quorum workers.
- Might not even be possible with prost, doesn't sound like it by researching on the web.
- Consider pure io_uring or glommio / monoio.

TODOs update 2;
1. Implement query data structure for reads such as btree.
  - When commit index is updated, the associated log entry gets applied to the read data structure.
2. Propagate commit index in append entries after persisted on quorum of nodes.
3. Metrics / Tracing to help identify performance bottlenecks.
4. Don't copy/allocate serialized data for persistence worker and quorum workers.
   - Might not even be possible with prost, doesn't sound like it by researching on the web.
   - Consider pure io_uring or glommio / monoio.
5. Implement CRUD operations
6. Refactor
7. Rename since Kraft is taken already.

Maybe when serving read requests the response is prepared but is only served when follower gets a hearbeat from the leader indicating that it's up to date.



Commit index work:
All nodes should append to the replication log after they've saved the data to disk.
When the leader receives successful append entries responses from a quorum of nodes it should update the commit index.
The current commit index should be sent in each append entries request.
If a follower receives an append entries request with a higher commit index that its own, it should update it.

When a node start up it should learn the commit index from the current leader.


If a node that just started receives append entries request from the current leader with a prev index and term that's lower than its own.
It should undo its own log last log entries until it reaches the prev index and term of the requestor.

Candidate state should delegate to follower state if receiving append entries request from a valid leader. 
Probably happens automatically given that the first request from the leader is always the heartbeat which would trigger a state change from Candidate.


persistence worker, quorum worker, read worker all need concurrent read access to transaction log
leader worker needs concurrent write access to transaction log.

On write:
1. State machine worker constructs an Arc LogEntry
2. It adds it to its transaction log i.e Vec Arc LogEntry
3. It sends the Arc LogEntry to the message bus.
4. The Quorum worker gets the entry from the bus and sends it to quorum members
5. The Persistence worker gets the entry from the bus and stores it on disk.
6. The Read worker gets the entry from the bus and applies it to the read datastructure if the commit index has been updated.

The read worker needs to be notified when the commit index changes. Maybe the SM worker and the read worker share an atomic value for the current term+commit index + a notifying primitive.

TODO
On boot all entries in the transaction log need to be sent over the bus to the query worker.
When node has become leader or follower with correct log for the first time it can emit all entries from the transaction log to the bus.
Before successfully joining the cluster and syncing its log with others certain entries may have to be undone.
Also possible that a log entry gets replicated to a less than a quorum and leader crashing when running in that case log entries may need to be reverted as well.

Maybe send initial query structure backfill whenever the initial leader election is done.
Maybe truncate query structure on truncate signal for simplicity and replay all non-truncated transactions.

For simplicity rebuild full query structure after a leader election when log truncation is done.
Would need a way to send a signal to the query worker that it should reset the structure from scratch, maybe send a log entry with term or index 0 and no data.

Need to hook into a good place in the state machine where the first OK append entries was completed.

On follower truncate, reset commit state, send log entry over bus with 0 term, index and data and backfill from start.
On boot backfill from start optimistically and only invalidate on truncate.