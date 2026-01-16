Distributed durable key-value database management system.
The core is implemented using the Raft consensus algorithm and utilises leader election and log replication.

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

Maybe when serving read requests the response is prepared but is only served when follower gets a hearbeat from the leader indicating that it's up to date.

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


TODO set term start after append entries if previous term != append entries term.