import AVFoundation
import Testing
@testable import RTMPHaishinKit

@Suite struct RTMPCompressedCompositionTimeTests {
    @Test func explicitCompressedTimePreservesExactPTSMinusDTSAndLegacyDefault() throws {
        let equal = try sample(pts: 1, dts: 1)
        let explicit = try #require(RTMPVideoMessage(streamId: 1, timestamp: 0,
            sampleBuffer: equal, compositionTimeOffset: 0))
        #expect(Array(explicit.payload[2..<5]) == [0, 0, 0])
        let reordered = try #require(RTMPVideoMessage(streamId: 1, timestamp: 0,
            sampleBuffer: sample(pts: 1.1, dts: 1), compositionTimeOffset: 0))
        #expect(Array(reordered.payload[2..<5]) == [0, 0, 100])
        let legacy = try #require(RTMPVideoMessage(streamId: 1, timestamp: 0, sampleBuffer: equal))
        #expect(Array(legacy.payload[2..<5]) == [0, 0, 66])
        #expect(explicit.payload.suffix(6) == legacy.payload.suffix(6))
    }

    private func sample(pts: Double, dts: Double) throws -> CMSampleBuffer {
        var block: CMBlockBuffer?
        let bytes: [UInt8] = [0, 0, 0, 2, 0x65, 0x88]
        try #require(CMBlockBufferCreateWithMemoryBlock(allocator: nil, memoryBlock: nil,
            blockLength: bytes.count, blockAllocator: nil, customBlockSource: nil,
            offsetToData: 0, dataLength: bytes.count, flags: 0, blockBufferOut: &block) == noErr)
        let data = try #require(block)
        try #require(bytes.withUnsafeBytes { CMBlockBufferReplaceDataBytes(with: $0.baseAddress!,
            blockBuffer: data, offsetIntoDestination: 0, dataLength: $0.count) } == noErr)
        var description: CMVideoFormatDescription?
        try #require(CMVideoFormatDescriptionCreate(allocator: nil, codecType: kCMVideoCodecType_H264,
            width: 1280, height: 720, extensions: nil, formatDescriptionOut: &description) == noErr)
        var timing = CMSampleTimingInfo(duration: CMTime(value: 1, timescale: 30),
            presentationTimeStamp: CMTime(seconds: pts, preferredTimescale: 1000),
            decodeTimeStamp: CMTime(seconds: dts, preferredTimescale: 1000))
        var length = bytes.count
        var result: CMSampleBuffer?
        try #require(CMSampleBufferCreateReady(allocator: nil, dataBuffer: data,
            formatDescription: description, sampleCount: 1, sampleTimingEntryCount: 1,
            sampleTimingArray: &timing, sampleSizeEntryCount: 1, sampleSizeArray: &length,
            sampleBufferOut: &result) == noErr)
        return try #require(result)
    }
}
