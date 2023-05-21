## Raft Consensus Algorithm

This project is an implementation of the core features of the Raft Consensus Algorithm proposed in
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) using Akka Actors.

### Usage

To run the algorithm, install [sbt](https://www.scala-sbt.org/) and execute the following command:

```
sbt "run <number_of_nodes>"
```

where `<number_of_processes>` is replaced by a positive integer value that is greater than 1.

You can also run an sbt shell, and then just type "compile", "run", or "runMain ..." in the resulting shell.

To terminate the algorithm, press the `Enter` key. This sends a shutdown message to all nodes so they can cleanly exit.

This launches an interactive shell where you can enter commands that the client will then forward to the cluster.
Our Raft implementation has built on top of it a simple Key/Value store. These are the commands available:

#### Client Operations Available

- `create <key> <value>` - Replace `<key>` and `<value>` with any arbitrary strings. This tells the client to
  send a ClientRequestRPC with the command to add `<value>` to our store associated with the `<key>` provided.

- `read <key>` - Replace `<key>` with any string. This tells the client to send a ClientQueryRPC to read a value from
  the store using `<key>`.

- `update <key> <value>` - Replace `<key>` and `<value>` with any arbitrary strings. This tells the client to
  send a ClientRequestRPC with the command to modify an existing entry's value associated with `<key>` to `<value>`. If
  the entry does not exist this operation will create a new entry.

- `delete <key>` - Replace `<key>` with any arbitrary string. This tells the client to send a ClientRequestRPC to delete
  the entry from the store associated with the `<key>`.

- `<string>` - If the commands are none of the ones above we treat it as a NoOp and send a request anyway.

#### Testing Operations Available

- `kill <node_id>` - Where `0 <= node_id < num_processes`. This is used to disconnect a node from the cluster and discard transient state
- `start <node_id>` - Where `0 <= node_id < num_processes`. This is used to reconnect a disconnected node to the cluster.

### Known Issues

- Only one client request can be serviced at a time.
- A configuration with only one node fails because some changes are made in response to events from other nodes