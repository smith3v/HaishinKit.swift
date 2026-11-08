import CoreMedia

/// Explicit compressed-input clock. Each track's deltas are measured from one
/// origin instead of independently rebasing its first media packet to zero.
/// Integer millisecond positions avoid a track-specific fractional accumulator.
struct RTMPSharedTimestamp {
    enum Error: Swift.Error { case invalidTime, invalidSequence, outOfRange }
    let origin: CMTime
    private var videoTime: CMTime?
    private var audioTime: CMTime?
    private var videoMilliseconds: Int64 = 0
    private var audioMilliseconds: Int64 = 0

    init(origin: CMTime) throws {
        guard origin.isNumeric else { throw Error.invalidTime }
        self.origin = origin
    }

    mutating func update(_ time: CMTime, video: Bool) throws -> UInt32 {
        guard time.isNumeric, time >= origin else { throw Error.invalidTime }
        let previous = video ? videoTime : audioTime
        guard previous == nil || time > previous! else { throw Error.invalidSequence }
        let position = CMTimeConvertScale(time - origin, timescale: 1000, method: .roundTowardZero)
        guard position.isNumeric, position.value >= 0,
              position.value <= Int64(UInt32.max) else { throw Error.outOfRange }
        let previousMilliseconds = video ? videoMilliseconds : audioMilliseconds
        let delta = position.value - previousMilliseconds
        guard delta >= 0, delta <= Int64(UInt32.max) else { throw Error.outOfRange }
        if video { videoTime = time; videoMilliseconds = position.value }
        else { audioTime = time; audioMilliseconds = position.value }
        return UInt32(delta)
    }
}
