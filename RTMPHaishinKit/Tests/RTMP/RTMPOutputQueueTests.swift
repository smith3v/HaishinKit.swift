import Foundation
import Testing
@testable import RTMPHaishinKit

@Suite struct RTMPOutputQueueTests {
    private struct Packet: RTMPMessage {
        let type = RTMPMessageType.video
        let streamId: UInt32 = 1
        let timestamp: UInt32 = 0
        let payload: Data
        init(_ byte: UInt8 = 1, count: Int = 64) { payload = Data(repeating: byte, count: count) }
    }

    @Test func stalledSendRetainsItsCreditAndOverflowClosesWholeGeneration() throws {
        let queue = RTMPOutputQueue(byteLimit: 300, messageLimit: 2, chunkSize: 128)
        let outcomes = Locked((success: 0, failure: 0))
        let completion: @Sendable (Result<Void, RTMPOutputQueue.Failure>) -> Void = { result in
            outcomes.withLock {
                switch result { case .success: $0.success += 1; case .failure: $0.failure += 1 }
            }
        }
        #expect(queue.offer(.zero, chunkStreamId: .video, message: Packet(0), completion: completion) == 64)
        let header = try #require(queue.take())
        #expect(queue.offer(.one, chunkStreamId: .video, message: Packet(1), completion: completion) == 64)
        #expect(queue.metrics.retainedMessages == 2)
        #expect(queue.metrics.retainedBytes <= 300)
        // Neither the task handing off a packet nor take() frees capacity.
        #expect(queue.offer(.one, chunkStreamId: .video, message: Packet(2), completion: completion) == 0)
        #expect(queue.metrics.failed)
        #expect(outcomes.withLock { $0.failure } == 3)
        #expect(queue.take() == nil)
        // The socket still owns the header until its completion; no false zero.
        #expect(queue.metrics.retainedMessages == 1)
        #expect(queue.metrics.retainedBytes > 0)
        #expect(throws: RTMPOutputQueue.Failure.self) { try queue.reset() }
        queue.acknowledge(header)
        queue.acknowledge(header)
        #expect(outcomes.withLock { $0.success } == 0)
        #expect(outcomes.withLock { $0.failure } == 3)
        #expect(queue.metrics.retainedBytes == 0)
        try queue.reset()
        #expect(!queue.metrics.failed)
        #expect(queue.offer(.zero, chunkStreamId: .video, message: Packet(3)) == 64)
        queue.acknowledge(header) // stale completion cannot credit new generation
        #expect(queue.metrics.retainedMessages == 1)
    }

    @Test func headerPayloadOrderAndCompletionAreAcknowledged() async throws {
        let queue = RTMPOutputQueue(byteLimit: 4096, messageLimit: 8, chunkSize: 128)
        let completed = Locked([Int]())
        queue.offer(.zero, chunkStreamId: .video, message: Packet(0)) { _ in completed.withLock { $0.append(0) } }
        queue.offer(.one, chunkStreamId: .video, message: Packet(1)) { _ in completed.withLock { $0.append(1) } }
        let header = try #require(queue.take())
        #expect(header.chunkType == .zero)
        #expect(header.message.payload.first == 0)
        #expect(queue.take() == nil)
        #expect(completed.withLock { $0.isEmpty })
        queue.acknowledge(header)
        let payload = try #require(queue.take())
        #expect(payload.chunkType == .one)
        #expect(payload.message.payload.first == 1)
        queue.acknowledge(payload)
        #expect(completed.withLock { $0 } == [0, 1])
        #expect(queue.metrics.retainedBytes == 0)
    }

    @Test func closeAndSocketCompletionResumeEachWaiterExactlyOnce() throws {
        let queue = RTMPOutputQueue(byteLimit: 65_536, messageLimit: 128, chunkSize: 128)
        let calls = Locked([Int: Int]())
        for id in 0..<64 {
            queue.offer(.one, chunkStreamId: .video, message: Packet()) { _ in
                calls.withLock { $0[id, default: 0] += 1 }
            }
        }
        let inFlight = try #require(queue.take())
        DispatchQueue.concurrentPerform(iterations: 128) { id in
            if id.isMultiple(of: 2) { queue.close(.cancelled) }
            else { queue.acknowledge(inFlight) }
        }
        #expect(calls.withLock { $0.count } == 64)
        #expect(calls.withLock { $0.values.allSatisfy { $0 == 1 } })
        #expect(queue.metrics.retainedBytes == 0)
        #expect(queue.metrics.failed)
    }

    @Test func cancellationFailsAwaitedSendAndDoesNotReleaseUnprocessedBytes() async throws {
        let queue = RTMPOutputQueue(byteLimit: 4096, messageLimit: 4, chunkSize: 128)
        var wakeups = queue.wakeups.makeAsyncIterator()
        let sender = Task { try await queue.send(.one, chunkStreamId: .video, message: Packet()) }
        _ = await wakeups.next()
        let inFlight = try #require(queue.take())
        sender.cancel()
        do { try await sender.value; Issue.record("Cancelled send unexpectedly succeeded") }
        catch { #expect(error is RTMPOutputQueue.Failure) }
        #expect(queue.metrics.failed)
        #expect(queue.metrics.retainedBytes > 0)
        queue.acknowledge(inFlight)
        #expect(queue.metrics.retainedBytes == 0)
    }

    @Test func idleConnectionDoesNotRetainItsOutputWorkers() async {
        weak var reference: RTMPConnection?
        do {
            let connection = RTMPConnection()
            reference = connection
        }
        #expect(reference == nil)
    }

    @Test func concurrentAdmissionCannotOvercommitBudget() {
        let queue = RTMPOutputQueue(byteLimit: 2048, messageLimit: 8, chunkSize: 128)
        DispatchQueue.concurrentPerform(iterations: 1000) { _ in
            queue.offer(.one, chunkStreamId: .video, message: Packet())
        }
        #expect(queue.metrics.failed)
        #expect(queue.metrics.highWaterBytes <= 2048)
        #expect(queue.metrics.highWaterMessages <= 8)
        #expect(queue.metrics.retainedBytes == 0)
    }
}

// Test counters use the same explicit lock ownership across supported OSes.
// No reference to mutable storage escapes the synchronous critical section.
private final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value
    init(_ value: Value) { self.value = value }
    func withLock<Result>(_ body: (inout Value) -> Result) -> Result {
        lock.withLock { body(&value) }
    }
}
