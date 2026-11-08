import CoreMedia
import Testing
@testable import RTMPHaishinKit

@Suite struct RTMPSharedTimestampTests {
    @Test func firstPacketOffsetsShareOneOriginRegardlessOfArrivalOrder() throws {
        let origin = CMTime(value: 10, timescale: 1)
        var clock = try RTMPSharedTimestamp(origin: origin)
        #expect(try clock.update(origin + CMTime(value: 35, timescale: 1000), video: false) == 35)
        #expect(try clock.update(origin, video: true) == 0)
        #expect(try clock.update(origin + CMTime(value: 1, timescale: 30), video: true) == 33)
        #expect(try clock.update(origin + CMTime(value: 2, timescale: 30), video: true) == 33)
        #expect(try clock.update(origin + CMTime(value: 3, timescale: 30), video: true) == 34)
    }
    @Test func aacRationalClockDoesNotAccumulateSeparateFractionDrift() throws {
        let origin = CMTime(value: 100, timescale: 1)
        var clock = try RTMPSharedTimestamp(origin: origin)
        var total: UInt64 = 0
        for frame in 0..<10_000 {
            let time = origin + CMTime(value: 2112 + Int64(frame) * 1024, timescale: 44_100)
            total += UInt64(try clock.update(time, video: false))
            #expect(total == UInt64(CMTimeConvertScale(time - origin, timescale: 1000, method: .roundTowardZero).value))
        }
    }
    @Test func invalidBackwardsPreoriginAndHugeTimesFailWithoutMutatingClock() throws {
        let origin = CMTime(value: 5, timescale: 1)
        #expect(throws: RTMPSharedTimestamp.Error.self) { try RTMPSharedTimestamp(origin: .invalid) }
        var clock = try RTMPSharedTimestamp(origin: origin)
        #expect(throws: RTMPSharedTimestamp.Error.self) { try clock.update(.invalid, video: true) }
        #expect(throws: RTMPSharedTimestamp.Error.self) { try clock.update(origin - CMTime(value: 1, timescale: 1), video: true) }
        #expect(try clock.update(origin, video: true) == 0)
        #expect(throws: RTMPSharedTimestamp.Error.self) { try clock.update(origin, video: true) }
        #expect(throws: RTMPSharedTimestamp.Error.self) { try clock.update(origin + CMTime(value: Int64(UInt32.max) + 1, timescale: 1000), video: true) }
        #expect(try clock.update(origin + CMTime(value: 1, timescale: 1), video: true) == 1000)
    }
    @Test func newTransportEpochDoesNotInheritOldTrackDeltas() throws {
        var first = try RTMPSharedTimestamp(origin: CMTime(value: 1, timescale: 1))
        #expect(try first.update(CMTime(value: 20, timescale: 1), video: true) == 19_000)
        var reconnected = try RTMPSharedTimestamp(origin: CMTime(value: 20, timescale: 1))
        #expect(try reconnected.update(CMTime(value: 20, timescale: 1), video: true) == 0)
        #expect(try reconnected.update(CMTime(value: 20_021, timescale: 1000), video: false) == 21)
    }
}
