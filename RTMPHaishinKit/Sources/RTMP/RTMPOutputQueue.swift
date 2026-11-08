import Foundation

/// An internal connection-wide mailbox, not a media frame queue. Admission is
/// synchronous before any output Task retains payloads. Bytes include the one
/// in-flight message until socket completion. The two wake streams retain only
/// scalar notifications, never media.
///
/// Ownership proof for Sendable: every mutable field is inside State and all
/// access goes through lock.withLock; continuations are removed under the lock
/// and resumed outside it. Record/Lease snapshots contain only Sendable values.
/// NSLock preserves the package's existing deployment targets (Mutex needs newer
/// OS versions). Concurrent admission/close/ack tests verify exactly-once release.
final class RTMPOutputQueue: @unchecked Sendable {
    enum Failure: Swift.Error, Sendable, Equatable { case closed, overflow, cancelled, sendFailed }
    struct Message: RTMPMessage {
        let type: RTMPMessageType
        let streamId: UInt32
        let timestamp: UInt32
        let payload: Data
    }
    struct Lease: Sendable {
        let id: UInt64
        let generation: UInt64
        let chunkType: RTMPChunkType
        let chunkStreamId: RTMPChunkStreamId
        let message: Message
    }
    struct Metrics: Sendable {
        let retainedBytes: Int
        let retainedMessages: Int
        let highWaterBytes: Int
        let highWaterMessages: Int
        let generation: UInt64
        let failed: Bool
        let failure: Failure?
    }
    private struct Record {
        let lease: Lease
        let cost: Int
        let completion: (@Sendable (Result<Void, Failure>) -> Void)?
    }
    private struct State {
        var generation: UInt64 = 0
        var nextID: UInt64 = 0
        var queued: [UInt64] = []
        var records: [UInt64: Record] = [:]
        var bytes = 0
        var inFlight: UInt64?
        var failure: Failure?
        var highWaterBytes = 0
        var highWaterMessages = 0
    }
    let byteLimit: Int
    let messageLimit: Int
    private let chunkSize: Int
    private let lock = NSLock()
    private var state = State()
    let wakeups: AsyncStream<Void>
    private let wake: AsyncStream<Void>.Continuation
    let failures: AsyncStream<UInt64>
    private let fail: AsyncStream<UInt64>.Continuation

    init(byteLimit: Int, messageLimit: Int, chunkSize: Int) {
        precondition(byteLimit > 0 && messageLimit > 0 && chunkSize > 0)
        self.byteLimit = byteLimit
        self.messageLimit = messageLimit
        self.chunkSize = min(chunkSize, 128)
        (wakeups, wake) = AsyncStream.makeStream(bufferingPolicy: .bufferingNewest(1))
        (failures, fail) = AsyncStream.makeStream(bufferingPolicy: .bufferingNewest(1))
    }
    deinit { wake.finish(); fail.finish() }
    func shutdown() { close(.closed); wake.finish(); fail.finish() }

    var metrics: Metrics {
        lock.withLock {
            Metrics(retainedBytes: state.bytes, retainedMessages: state.records.count,
                highWaterBytes: state.highWaterBytes, highWaterMessages: state.highWaterMessages,
                generation: state.generation, failed: state.failure != nil, failure: state.failure)
        }
    }

    /// Must follow cancellation/closure of the preceding socket. Old completions
    /// carry a generation and cannot release credit from a new connection.
    func reset() throws {
        close(.closed)
        try lock.withLock {
            // Never reuse credit while a previous socket still retains a send.
            // Callers may create a fresh connection, or retry after cancellation
            // completion. This cannot block a new recorder or wait indefinitely.
            guard state.inFlight == nil else { throw Failure.closed }
            state.generation &+= 1
            state.failure = nil
        }
    }

    @discardableResult
    func offer(_ type: RTMPChunkType, chunkStreamId: RTMPChunkStreamId, message: some RTMPMessage,
               completion: (@Sendable (Result<Void, Failure>) -> Void)? = nil) -> Int {
        // The payload is a value snapshot. No Task/continuation captures it before
        // the byte/message check. Reserve conservative RTMP chunk header overhead.
        let payload = message.payload
        let count = payload.count
        let overhead = ((count / chunkSize) + 1) * 18 + 32
        let (cost, overflow) = count.addingReportingOverflow(overhead)
        var callbacks: [@Sendable (Result<Void, Failure>) -> Void] = []
        var failure: Failure?
        var failedGeneration: UInt64?
        let accepted = lock.withLock { () -> Bool in
            if let existing = state.failure { failure = existing; return false }
            guard !overflow, cost <= byteLimit, state.bytes <= byteLimit - cost,
                  state.records.count < messageLimit else {
                state.failure = .overflow
                failedGeneration = state.generation
                callbacks = state.records.values.compactMap(\.completion)
                retainOnlyInFlight(&state)
                failure = .overflow
                return false
            }
            let id = state.nextID
            state.nextID &+= 1
            let snapshot = Message(type: message.type, streamId: message.streamId,
                timestamp: message.timestamp, payload: payload)
            let lease = Lease(id: id, generation: state.generation, chunkType: type,
                chunkStreamId: chunkStreamId, message: snapshot)
            state.records[id] = Record(lease: lease, cost: cost, completion: completion)
            state.queued.append(id)
            state.bytes += cost
            state.highWaterBytes = max(state.highWaterBytes, state.bytes)
            state.highWaterMessages = max(state.highWaterMessages, state.records.count)
            return true
        }
        if accepted { wake.yield(()); return count }
        if let failure {
            callbacks.forEach { $0(.failure(failure)) }
            completion?(.failure(failure))
        }
        if let failedGeneration { fail.yield(failedGeneration) }
        return 0
    }

    func send(_ type: RTMPChunkType, chunkStreamId: RTMPChunkStreamId,
              message: some RTMPMessage) async throws {
        try Task.checkCancellation()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Swift.Error>) in
                offer(type, chunkStreamId: chunkStreamId, message: message) {
                    continuation.resume(with: $0.mapError { $0 as any Swift.Error })
                }
            }
        } onCancel: {
            // RTMP cannot safely cancel/drop a partially written message. End the
            // transport generation, freeing every waiter and forcing reconnect.
            self.close(.cancelled)
        }
    }

    func take() -> Lease? {
        lock.withLock {
            guard state.failure == nil, state.inFlight == nil, !state.queued.isEmpty else { return nil }
            let id = state.queued.removeFirst()
            state.inFlight = id
            return state.records[id]?.lease
        }
    }

    func acknowledge(_ lease: Lease) {
        let completion = lock.withLock { () -> (@Sendable (Result<Void, Failure>) -> Void)? in
            guard lease.generation == state.generation,
                  let record = state.records.removeValue(forKey: lease.id) else { return nil }
            state.bytes -= record.cost
            state.inFlight = nil
            return record.completion
        }
        completion?(.success(()))
    }

    func close(_ failure: Failure) {
        var generation: UInt64?
        let callbacks = lock.withLock { () -> [@Sendable (Result<Void, Failure>) -> Void] in
            guard state.failure == nil else { return [] }
            state.failure = failure
            generation = state.generation
            let result = state.records.values.compactMap(\.completion)
            retainOnlyInFlight(&state)
            return result
        }
        callbacks.forEach { $0(.failure(failure)) }
        if let generation { fail.yield(generation) }
    }
    /// On close, the socket may still retain one lease until contentProcessed.
    /// Keep charging it, but strip its already-resumed completion.
    private func retainOnlyInFlight(_ state: inout State) {
        if let id = state.inFlight, let record = state.records[id] {
            state.records = [id: Record(lease: record.lease, cost: record.cost, completion: nil)]
            state.bytes = record.cost
        } else { state.records.removeAll(); state.bytes = 0 }
        state.queued.removeAll()
    }

}
