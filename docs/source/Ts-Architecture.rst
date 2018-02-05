.. _Architecture:

.. Contents::

T-streams Architecture
============================

At this page, we will dive into the T-streams architecture which helps to understand the basic operations. This part will give you a general idea of operation handling.

Overview
------------------

The T-streams architecture is quite simple. Its design is inspired by Apache Kafka. Though the implementation enables us to fulfill the requirements - fault-tolerance, scalability, eventual consistency -
offering competitive performance in transactional messaging.

T-streams includes the following components:

1. **Storage Server** that is responsible for storing transactions and their data. Server contains an embedded storage and a commit log. Embedded storage is implemented with `RocksDB <http://rocksdb.org/>`_. A commit log can be a local file or a distributed commit log service provided by `Apache BookKeeper <https://bookkeeper.apache.org/>`_.
#. **Producers** that create transactions with data.
#. **Consumers**, **Subscribers** that read the transactions.

You can see the T-streams component correlation in the figure below:

.. figure:: _static/Architecture_GeneralStructure.png

T-streams can be configured in two modes:

1) In a single-node mode providing non-fault tolerant processing;
2) In a multi-node mode providing 
     
     - fault-tolerant processing by backing up servers,
     - scalability implemented with sharded servers.

Any configuration requires infrastructure. In the next section, you will find more information on infrastructure components in T-streams.

Infrastructure Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following infrastructure components are required in T-streams:

1. **Apache ZooKeeper** that is responsible for coordination and synchronization of processes.
#. **RocksDB** that is an embedded database fulfilling a very important feature – an atomic batch operation which allows implementing atomic and reliable commit logs processing. 
#. **Apache BookKeeper** used as a distributed commit log. It is a service that provides a persistent, fault-tolerant log-structured storage. BookKeeper is an optional part used in the multi-node mode. It orderly stores elements and replicates them across servers to synchronize the servers' states. In a single-node configuration BookKeeper is not required. Server's local commit log file is used instead.

At the figure below T-streams infrastructure is presented: 

.. figure:: _static/Architecture-General2.png

Thus, Server publishes an endpoint to ZooKeeper. Agents (eg. Producers and Consumers) read the endpoint, discover Server and connect to it for creating/reading transactions with data. 

Producers write the operations, transactions with data and meta-data to local commit log, or to BookKeeper in the multi-node configuration, and stores data to Server's embedded storage. Consumers and Subscribers read these transactions and their data from the Server's storage. See the Data_Flow_ section for more details on data flow in T-streams.

This is a simplified description of T-streams architecture and operations. Next, we will gain a deeper insight into T-streams key component - T-streams Transaction Storage Server.

T-streams Transaction Storage Server
--------------------------------------

The T-streams Transaction Storage Server (TTS) is an external process which keeps transactions and their data safe. Its abstract architecture can be represented in the following way:

.. figure:: _static/Architecture-TTS.png

The Server contains the internal parts: 
 
 - Request Handler; 
 - Commit Log Writer; 
 - Commit Log Reader;
 - Commit Log that can be implemetned as a local file or BookKeeper;
 - RocksDB storage.
 
Request Handler processes all operations (i.e. create, retrieve, update, delete a transaction) and write them to Commit Log via Commit Log Writer. 

Commit Log Writer orderly writes the operations to Server's local commit log or BookKeeper. Then Commit Log Reader reads the operations from the commit log and stores them to RocksDB. Receiving data request, Request Handler reads transactions and their data from RocksDB to return them to an agent (Consumer or Subscriber).

In the single-node implementation, the recording of transaction operations differs from the recording of data operations. Commit Log Writer records meta-data to the commit log, storing data (if any exist in an operation) directly to RocksDB. In the figure below the meta-data recording is displayed with green arrows. Data recording flow is displayed with red arrows. 

.. figure:: _static/Architecture-SingleNode1.png

Agents discover the Leader server via Apache ZooKeeper. ZooKeeper returns Server's IP address. Using it, agents connect to the Server to perform the operations.

.. figure:: _static/Architecture-Server-OneNode.png

In the fault-tolerant mode implementation, ZooKeeper returns the address of the Leader server to agents. Agents perform operations on Leader that registers them in BookKeeper commit log and stores data to the storage. Followers read from BookKeeper to synchronize their state with the Leader. 

.. figure:: _static/Architecture-ServerLeader.png

In case Leader is down or unavailable, one of the Followers becomes a Leader server. Its address is returned to agents to send all operations to. Once the former Leader is recovered, it becomes a Follower and starts reading the data that is written by the new Leader in BookKeeper.

.. figure:: _static/Architecture-ServerFollower.png

In the fault-tolerant mode implementation, one Leader and one or more Followers can be deployed. In a most common scenario, one Leader and one Follower are in a cluster. 

Servers can be backed up. In this case, we will speak about a scalable mode that is described below.

The T-streams Transaction Storage Server is a sub-project which can be found on `GitHub <https://github.com/bwsw/t-streams/tree/develop/tstreams-transaction-server>`_.

Scalable Configuration
------------------------

T-streams allows operating in a scalable mode. It is possible in case data processing is implemented via more than one stream as a single T-streams stream is not scalable. 

Each stream is assigned to a Leader server. For example, there are 3 streams in the process - Stream 1, Stream 2 and Stream 3. Each of the streams is assigned to a Server. So we involve 3 servers into the processing. Producer 1 working with Stream 1 connects to Server 1. Producer 2 working with Stream 2 connects to Server 2. Producer 3 working with Stream 3 connects to Server 3. These servers have a Common role. Servers with the Common role save operations for an individual transaction (i.e. create, retrieve, update, delete a transaction).

Group checkpoint operations (Producer object, Checkpoint Group object), which are common for several transactions or agents in the process, will be sent to a separate server. At the figure below it is marked as CG. This server has a CheckpointGroup role.  So all Producers in the runtime will connect to the CheckointGroup server as well to send Producer or Checkpoint Group object operations.

.. figure:: _static/Architecture_Scale1.png

The checkpoint operation allows fixing a lot of transactions as a single operation. Frequent checkpointing leads to a slowdown in performance, so it is preferable to do checkpoint as rare as possible and use group checkpoint operations.

.. _Data_Flow:

Data Flow
-------------------

Now having a general idea of the T-streams architecture you can easily understand the data flow in T-streams. 

Look at the figures below. They demonstrate the data flow between a Producer and a Subscriber. 

Let us consider them step by step. 

1) Once Subscriber starts, it registers in Apache ZooKeeper (1.1). Zookeeper provides Producers with the list of Subscribers in the stream (1.2). 

2) Producer sends an open transaction request to Server (2.1). Server opens a transaction (``txn1``) and writes the operation to Commit Log (2.2). Then it returns an acknowledgment to Producer. Producer sends an open event to Subscriber to inform it of the ``txn1`` transaction opening (2.3). 

3) Producer puts data for the ``txn1`` transaction and they are stored to Commit Log and to RocksDB (3.2, 3.3).

.. figure:: _static/Architecture-DataFlow_Prod4.png

Once all data are stored for the transaction, they get available to Subscriber. It is fulfilled with the following operations:

1) Producer performs transaction checkpoint/canceling (1.1). Server writes the checkpoint/cancel operation to Commit Log and sends an acknowledgement to Producer (1.2). After receiving the acknowledgment of the ``txn1`` checkpoint/cancel, Producer sends the transaction checkpoint/cancel notification to Subscriber (1.3).

2) Subscriber receives the checkpoint event and gets informed of ``txn1`` being checkpointed. Or in case of Cancel operation, Subscriber receives notification the ``txn1`` transaction is canceled. Now Subscriber can request Server for data in ``txn1`` (2.1).

3) Server reads data from RocksDB (2.2) and returns them to Subscriber (2.3)

.. figure:: _static/Architecture-DataFlow_Subscr5.png

Read Next
----------------

At the Getting_Started_ page you will get instructions on how to include prebuilt T-streams library dependency in your project from a public maven repository. 

If you are aimed to participate in the development process, you should read the Build_T-streams_ page to find the details on how to build T-streams library in GNU/Linux environment.

T-streams library provides a developer with a range of top-level objects used to perform operations. Visit API_ page to study the list of API methods.
