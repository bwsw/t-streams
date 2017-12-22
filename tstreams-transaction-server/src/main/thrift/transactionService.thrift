namespace scala com.bwsw.tstreamstransactionserver.rpc

enum TransactionStates {
    Opened       = 1
    Updated      = 2
    Cancel       = 3
    Invalid      = 4
    Checkpointed = 5
}

typedef i32    StreamIDType
typedef i32    PartitionType
typedef i64    transactionIDType
typedef i32    tokenType
typedef i64    tllType

struct CommitLogInfo {
  1: required i64 currentProcessedCommitLog
  2: required i64 currentConstructedCommitLog
}

struct ProducerTransaction {
   1: required StreamIDType        stream
   2: required PartitionType       partition
   3: required transactionIDType   transactionID
   4: required TransactionStates   state
   5: required i32                 quantity
   6: required tllType             ttl
}


struct ProducerTransactionsAndData {
   1: list<ProducerTransaction> producerTransactions,
   2: list<binary> data
}

struct ConsumerTransaction {
   1: required StreamIDType        stream
   2: required PartitionType       partition
   3: required transactionIDType   transactionID
   4: required string              name
}

struct Transaction {
    1: optional ProducerTransaction    producerTransaction
    2: optional ConsumerTransaction    consumerTransaction
}

struct StreamValue {
    1: required string        name
    2: required i32           partitions
    3: optional string        description
    4: required tllType       ttl
    5: optional string        zkPath
}

struct Stream {
    1: required StreamIDType  id
    2: required string        name
    3: required i32           partitions
    4: optional string        description
    5: required tllType       ttl
    6: required string        zkPath
}

struct TransportOptionsInfo {
    1: required i32 maxMetadataPackageSize
    2: required i32 maxDataPackageSize
}

struct ScanTransactionsInfo {
    1: required transactionIDType           lastOpenedTransactionID
    2: required list<ProducerTransaction>   producerTransactions
}

struct TransactionInfo {
    1: required bool                 exists
    2: optional ProducerTransaction  transaction
}

exception ServerException {
    1: string message;
}



service StreamService {

  bool putStream(1: string name, 2: i32 partitions, 3: optional string description, 4: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: string name) throws (1:ServerException error),

  Stream getStream(1: string name) throws (1:ServerException error),

  bool delStream(1: string name) throws (1:ServerException error)
}

service TransactionIDService {
  transactionIDType getTransactionID() throws (1:ServerException error),

  transactionIDType getTransactionIDByTimestamp(1: transactionIDType timestamp) throws (1:ServerException error),
}



service TransactionMetaService {

   bool putTransaction(1: Transaction transaction) throws (1:ServerException error),

   bool putTransactions(1: list<Transaction> transactions) throws (1:ServerException error),

   bool putProducerStateWithData(1: ProducerTransaction transaction, 2: list<binary> data, 3: i32 from) throws (1:ServerException error),

   transactionIDType putSimpleTransactionAndData(1: StreamIDType streamID, 2: PartitionType partition, 3: list<binary> data) throws (1:ServerException error),

   transactionIDType openTransaction(1: StreamIDType streamID, 2: PartitionType partition, 3: i64 transactionTTLMs) throws (1:ServerException error),

   ScanTransactionsInfo scanTransactions(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType from, 4: transactionIDType to, 5: i32 count, 6: set<TransactionStates> states) throws (1:ServerException error),

   TransactionInfo getTransaction(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction) throws (1:ServerException error),

   transactionIDType getLastCheckpointedTransaction(1: StreamIDType streamID, 2: PartitionType partition) throws (1:ServerException error)
}



service TransactionDataService {

  bool putTransactionData(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction, 4: list<binary> data, 5: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction, 4: i32 from, 5: i32 to) throws (1:ServerException error)
}


service ConsumerService {

 bool putConsumerCheckpoint(1: string name, 2: StreamIDType streamID, 3: PartitionType partition, 4: transactionIDType transaction) throws (1:ServerException error),

 i64 getConsumerState(1: string name, 2: StreamIDType streamID, 3: PartitionType partition) throws (1:ServerException error)
}


service authService {

  tokenType authenticate(1: string authKey),

  bool isValid(1: tokenType token)
}

service transportService {

  TransportOptionsInfo getMaxPackagesSizes(),

  string getZKCheckpointGroupServerPrefix()
}


service TransactionService {

  CommitLogInfo getCommitLogOffsets() throws (1:ServerException error)

  StreamIDType putStream(1: string name, 2: i32 partitions, 3: optional string description, 4: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: string name) throws (1:ServerException error),

  Stream getStream(1: string name) throws (1:ServerException error),

  bool delStream(1: string name) throws (1:ServerException error),

  transactionIDType getTransactionID() throws (1:ServerException error),

  transactionIDType getTransactionIDByTimestamp(1: transactionIDType timestamp) throws (1:ServerException error),

  bool putTransaction(1: Transaction transaction) throws (1:ServerException error),

  bool putTransactions(1: list<Transaction> transactions) throws (1:ServerException error),

  bool putProducerStateWithData(1: ProducerTransaction transaction, 2: list<binary> data, 3: i32 from) throws (1:ServerException error),

  transactionIDType putSimpleTransactionAndData(1: StreamIDType streamID, 2: PartitionType partition, 3: list<binary> data) throws (1:ServerException error),

  transactionIDType openTransaction(1: StreamIDType streamID, 2: PartitionType partition, 3: i64 transactionTTLMs) throws (1:ServerException error),

  ScanTransactionsInfo scanTransactions(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType from, 4: transactionIDType to, 5: i32 count, 6: set<TransactionStates> states) throws (1:ServerException error),

  TransactionInfo getTransaction(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction) throws (1:ServerException error),

  transactionIDType getLastCheckpointedTransaction(1: StreamIDType streamID, 2: PartitionType partition) throws (1:ServerException error),

  bool putTransactionData(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction, 4: list<binary> data, 5: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: StreamIDType streamID, 2: PartitionType partition, 3: transactionIDType transaction, 4: i32 from, 5: i32 to) throws (1:ServerException error),

  bool putConsumerCheckpoint(1: string name, 2: StreamIDType streamID, 3: PartitionType partition, 4: transactionIDType transaction) throws (1:ServerException error),

  transactionIDType getConsumerState(1: string name, 2: StreamIDType streamID, 3: PartitionType partition) throws (1:ServerException error),

  tokenType authenticate(1: string authKey),

  bool isValid(1: tokenType token),

  TransportOptionsInfo getMaxPackagesSizes(),

  string getZKCheckpointGroupServerPrefix()
}
