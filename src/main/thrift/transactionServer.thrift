namespace scala com.bwsw.tstreams.transactionServer.rpc

enum TransactionStates {
    Opened       = 1
    Checkpointed = 2
    Invalid      = 3
}

service TransactionServerService {
    // auth api
    string authenticate(1: string login, 2: string password),

    // transaction api
    bool putTransaction(1: string token, 2: string stream, 3: i32 partition, 4: i64 interval, 5: i64 transaction, 6: TransactionStates state, 7: i32 quantity, 8: i32 timestamp),
}
