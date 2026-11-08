import Foundation
import Testing
@testable import RTMPHaishinKit

@Suite struct RTMPOutputOrderingTests {
    private actor Pause {
        nonisolated let entered: AsyncStream<Void>
        private let signal: AsyncStream<Void>.Continuation
        private var continuation: CheckedContinuation<Void, Never>?
        init() { (entered, signal) = AsyncStream.makeStream(bufferingPolicy: .bufferingNewest(1)) }
        func wait() async {
            await withCheckedContinuation { continuation in
                self.continuation = continuation
                signal.yield(())
            }
        }
        func release() { continuation?.resume(); continuation = nil }
    }
    private actor Wire {
        private(set) var bytes: [UInt8] = []
        func append(_ data: Data) { bytes.append(contentsOf: data) }
    }

    @Test func delayedHandshakeCompletionPrecedesQueuedConnectBytes() async throws {
        let pause = Pause()
        let wire = Wire()
        let socket = RTMPSocket(sendContent: { data in
            if data.first == 2 { await pause.wait() }
            await wire.append(data)
        })
        struct Connect: RTMPMessage {
            let type = RTMPMessageType.amf0Command
            let streamId: UInt32 = 0
            let timestamp: UInt32 = 0
            let payload = Data([3])
        }
        let queue = RTMPOutputQueue(byteLimit: 1024, messageLimit: 4, chunkSize: 128)
        var handshakeEntered = pause.entered.makeAsyncIterator()
        // This is the connection's real handshake completion contract: C2 must
        // finish processing before its caller can queue the connect command.
        let sender = Task {
            try await socket.sendAcknowledged([Data([2])])
            queue.offer(.zero, chunkStreamId: .command, message: Connect())
        }
        _ = await handshakeEntered.next()
        #expect(queue.metrics.retainedMessages == 0)
        #expect(await wire.bytes.isEmpty)
        await pause.release()
        try await sender.value
        let command = try #require(queue.take())
        try await socket.sendAcknowledged([command.message.payload])
        queue.acknowledge(command)
        #expect(await wire.bytes == [2, 3])
    }

    @Test func reconnectIsRejectedThroughoutPendingCloseCleanup() async throws {
        let connection = RTMPConnection()
        let pause = Pause()
        var closeEntered = pause.entered.makeAsyncIterator()
        let close = Task { await connection.finishClose { await pause.wait() } }
        _ = await closeEntered.next()
        do {
            _ = try await connection.connect("rtmps://127.0.0.1/unused")
            Issue.record("Reconnect entered a pending close operation")
        } catch {
            guard case RTMPConnection.Error.invalidState = error else {
                Issue.record("Reconnect did not fail at the cleanup guard")
                await pause.release()
                await close.value
                return
            }
        }
        await pause.release()
        await close.value
        #expect(await connection.connected == false)
        #expect(await connection.uri == nil)
        // A malformed URL distinguishes a released lifecycle guard from an
        // attempted network connection, without creating any real socket.
        do { _ = try await connection.connect("rtmps:/"); Issue.record("Malformed URL accepted") }
        catch {
            guard case RTMPConnection.Error.unsupportedCommand = error else {
                Issue.record("Lifecycle guard remained closed after cleanup")
                return
            }
        }
    }
}
