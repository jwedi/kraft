@0xcb46bdc9c04596e1;

# Tracing context for distributed tracing
struct TracingContextEntry {
  key @0 :Text;
  value @1 :Text;
}

struct TracingContext {
  entries @0 :List(TracingContextEntry);
}

# Response type enum
enum RemoteResponseType {
  unspecified @0;
  ok @1;
  invalid @2;
}

# Log entry for replication
struct RemoteLogEntry {
  index @0 :UInt64;
  data @1 :Data;  # Zero-copy bytes access
  batchIndex @2 :UInt64;
  term @3 :UInt64;
  prevLogTerm @4 :UInt64;
  prevLogIndex @5 :UInt64;
  messageId @6 :UInt64;
}

# Append entries (leader -> follower)
struct RemoteAppendEntriesRequest {
  term @0 :UInt64;
  leaderId @1 :UInt32;
  prevLogIndex @2 :UInt64;
  prevLogTerm @3 :UInt64;
  requestId @4 :UInt64;
  entry @5 :RemoteLogEntry;
  commitIndex @6 :UInt64;
}

struct RemoteAppendEntriesAcknowledge {
  lastLogTerm @0 :UInt64;
  lastLogIndex @1 :UInt64;
  requestId @2 :UInt64;
  ok @3 :Bool;
}

# Vote request/response (candidate -> all)
struct RemoteVoteRequest {
  term @0 :UInt64;
  candidateId @1 :UInt32;
  lastLogIndex @2 :UInt64;
  lastLogTerm @3 :UInt64;
  requestId @4 :UInt64;
}

struct RemoteVoteResponse {
  voteGranted @0 :Bool;
  requestId @1 :UInt64;
  term @2 :UInt64;
}

# Put request/response (client -> leader)
struct RemotePutRequest {
  id @0 :Text;
  payload @1 :Text;
  nodeId @2 :UInt32;
}

struct RemotePutBatchRequest {
  putRequests @0 :List(RemotePutRequest);
  batchId @1 :Text;
}

struct RemotePutResponse {
  id @0 :Text;
  responseType @1 :RemoteResponseType;
  message @2 :Text;
  nodeId @3 :UInt32;
  batchId @4 :Text;
}

struct RemotePutBatchResponse {
  responses @0 :List(RemotePutResponse);
  batchId @1 :Text;
}

# Connection handshake
struct RemoteConnectRequest {
  nodeId @0 :UInt32;
}

struct RemoteConnectResponse {
  ok @0 :Bool;
}

# Log truncation
struct RemoteTruncateLogRequest {
  prevTerm @0 :UInt64;
  prevIndex @1 :UInt64;
  term @2 :UInt64;
  leaderId @3 :UInt32;
  requestId @4 :UInt64;
}

struct RemoteTruncateLogResponse {
  requestId @0 :UInt64;
  ok @1 :Bool;
}

# Main message wrapper with union for payload types
struct RemoteQuorumMessage {
  messagePayload :union {
    connectRequest @0 :RemoteConnectRequest;
    connectResponse @1 :RemoteConnectResponse;
    voteRequest @2 :RemoteVoteRequest;
    voteResponse @3 :RemoteVoteResponse;
    appendEntriesRequest @4 :RemoteAppendEntriesRequest;
    appendEntriesAcknowledge @5 :RemoteAppendEntriesAcknowledge;
    putBatchRequest @6 :RemotePutBatchRequest;
    putBatchResponse @7 :RemotePutBatchResponse;
    truncateLogRequest @8 :RemoteTruncateLogRequest;
    truncateLogResponse @9 :RemoteTruncateLogResponse;
  }
  tracingContext @10 :TracingContext;
}

# Peer interface for receiving messages
interface RaftPeer {
  receiveMessage @0 (message :RemoteQuorumMessage) -> ();
}

# Main Raft interface for connection establishment
interface Raft {
  connect @0 (myPeer :RaftPeer) -> (remotePeer :RaftPeer);
}
