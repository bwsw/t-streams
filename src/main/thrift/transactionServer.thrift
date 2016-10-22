namespace scala com.bwsw.tstreams.transactionServer.rpc

enum TransactionStates {
    Opened       = 1
    Checkpointed = 2
    Invalid      = 3
}

struct Transaction {
    1: required string              stream
    2: required i32                 partition
    3: required i64                 interval
    4: required i64                 transactionID
    5: required TransactionStates   state
    6: required i32                 quantity
    7: required i32                 timestamp
}

struct Stream {
    1: required i32 partitions
    2: optional string description
}

service TransactionServerService {
    // transaction api
    bool putTransaction(1: string token, 2: list<Transaction> transactions),

    list<Transaction> scanTransactions(1: string token, 2: string stream, 3: i32 partition, 4: i64 interval),

    i32 scanTransactionsCRC32(1: string token, 2: string stream, 3: i32 partition, 4: i64 interval),

    bool delTransaction(1: string token, 2: string stream, 3: i32 partition, 4: i64 interval, 5: i64 transaction),

    bool putStream(1: string token, 2: string stream, 3: i32 partition, 4: i32 partitions, 5: string description),

    bool delStream(1: string token, 2: string stream),

    Stream getStream(1: string token, 2: string stream),

    bool setConsumerState(1: string token, 2: string name, 3: string stream, 4: i32 partition, 5: i64 transaction),

    i64 getConsumerState(1: string token, 2: string name, 3: string stream, 4: i32 partition),

    bool putTransactionData(1: string token, 2: string stream, 3: i32 partition, 4: i64 transaction, 5: i32 from, 6: i32 to, 7: list<binary> data),

    list <binary> getTransactionData(1: string token, 2: string stream, 3: i32 partition, 4: i64 transaction, 5: i32 from, 6: i32 to),

}
