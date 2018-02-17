# DHTDataStorage

A prototype of node failure resistant replicated Key Value storage system.
Introduction
This project is about implementing a simplified version of Dynamo. There are three main pieces that are implemented 1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation always performs read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. 

Implementation details:

Used SHA-1 as my hash function to generate keys.
Supports insert/query/delete operations. Queries like “@” (key-values in a particular node) and “*” (total key-values in the system) are supported.
All failures are temporary; a failed node will recover soon, i.e., it will not be permanently unavailable during a run.
When a node recovers, it copies all the object writes it missed during the failure. This is done by asking the right nodes and copying from them.
The content provider supports concurrent read/write operations and handles a failure happening at the same time with read/write operations.
Replication is done exactly the same way as Dynamo does. In other words, a (key, value) pair is replicated over three consecutive partitions, starting from the partition that the key belongs to.
All replicas store the same values for each key. This is “per-key” consistency. 
Each content provider instance has a node id derived from its emulator port. This node id is obtained by applying a hash function (i.e., genHash()) to the emulator port. For example, the node id of the content provider instance running on emulator-5554 should be, node_id = genHash(“5554”). This is necessary to find the correct position of each node in the Dynamo ring.
The system has fixed the ports & sockets.
Any app is able to access (read and write) the content provider.
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.
Request routing: Each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the coordinator should be in charge of serving read/write operations.
For linearizability, implemented a quorum-based replication used by Dynamo.
The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
Both the reader quorum size R and the writer quorum size W should be 2.
The coordinator for a get/put request always contacts other two nodes and get a vote from each (i.e., an acknowledgment for a write, or a value for a read).
For write operations, all objects are versioned in order to distinguish stale copies from the most recent copy.
For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator picks the most recent version and return it.
Another replication strategy you is implemented is the chain replication, which provides linearizability.
In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write.
A read operation always comes to the last partition and reads the value from the last partition.
Focus on correctness rather than performance. 

Failure handling:
Handling failures is done very carefully because there can be many corner cases to consider and cover.
Just as the original Dynamo, each request is used to detect a node failure.
For this purpose, a timeout is used for a socket read; A reasonable timeout value, e.g., 100 ms, is picked, and if a node does not respond within the timeout, the node is considered to be failed.
When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.

If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
