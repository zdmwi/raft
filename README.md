## Mixed Consistency Ticket Sales 

This project is an implementation of a Ticket Sales application that makes use of both consistent and inconsistent reads
on top of the core features of the Raft Consensus Algorithm proposed in
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) using Akka Actors.

### Usage

To run the algorithm, install [sbt](https://www.scala-sbt.org/) and execute the following command:

```
sbt "run <number_of_nodes>"
```

where `<number_of_nodes>` is replaced by a positive integer value that is greater than 1. For the sake of the application
we also spawn one frontend per available node. Frontends will make unstable read requests to the nodes with matching IDs
where the ID is just the index in the collection of nodes or collection of frontends.

You can also run an sbt shell, and then just type "compile", "run", or "runMain ..." in the resulting shell.

To terminate the algorithm, press the `Enter` key. This sends a shutdown message to all nodes so they can cleanly exit.

This launches an interactive shell where you can enter commands that the client will then forward to the cluster.
Our Raft implementation has built on top of it a simple Key/Value store. These are the commands available:

#### Client Operations Available

- `buy <value>` - Replace `<value>` with any positive integer. This tells the client to
  send a ClientRequestRPC with the command to buy `<value>` amount of tickets. All this does to our state is reduce 
  the number of remainingTickets by `<value>`.

- `read` - This tells the client to make a Consistent Read by sending a ClientQueryRPC to read the latest committed 
  ticket count from the state.

- `unread` - This tells the client to make an Inconsistent read to its closest replica by sending an UnstableClientQueryRPC
  to read a guess as to what the remaining ticket count is.

- `replenish <value>` - Replace `<value>` with any positive integer. This tells the client to replenish or add the 
  corresponding `<value>` amount of tickets to the remaining ticket count.

- `<string>` - If the commands are none of the ones above we treat it as a NoOp and send a request anyway.

#### Testing Operations Available

- `kill <node_id>` - Where `0 <= node_id < num_processes`. This is used to disconnect a node from the cluster and discard transient state
- `start <node_id>` - Where `0 <= node_id < num_processes`. This is used to reconnect a disconnected node to the cluster.
- `workload` - This is used to fill the request queues of the frontends with the commands specfied in the `Main.scala` file.

### Known Issues

- When state is recovered from the files, the buy operations are repeated and affect the last saved state. To 
  prevent this, please wipe the state clean before a restart. This happens because we don't persist the client 