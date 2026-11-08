import AVFoundation
import Testing
@testable import RTMPHaishinKit

@Suite struct RTMPNativeAACHeaderTests {
    @Test func canonicalNativeAACAndLegacyObjectIDProduceEquivalentSequenceHeader() throws {
        let canonical = try format(id: kAudioFormatMPEG4AAC, flags: 0)
        let legacy = try format(id: kAudioFormatMPEG4AAC, flags: UInt32(MPEG4ObjectID.AAC_LC.rawValue))
        let actual = try #require(RTMPAudioMessage(streamId: 1, timestamp: 0, formatDescription: canonical))
        let previous = try #require(RTMPAudioMessage(streamId: 1, timestamp: 0, formatDescription: legacy))
        // FLV AAC sequence header, AAC-LC, 44.1kHz, stereo. Raw AAC packet
        // payloads require this message before their first type-1 RTMP chunk.
        #expect(actual.payload == Data([0xaf, 0x00, 0x12, 0x10]))
        #expect(actual.payload == previous.payload)
        #expect(RTMPAudioMessage(streamId: 1, timestamp: 0,
            formatDescription: try format(id: kAudioFormatMPEG4AAC_HE, flags: 0)) == nil)
    }

    private func format(id: AudioFormatID, flags: AudioFormatFlags) throws -> CMAudioFormatDescription {
        var asbd = AudioStreamBasicDescription(mSampleRate: 44_100, mFormatID: id,
            mFormatFlags: flags, mBytesPerPacket: 0, mFramesPerPacket: 1024,
            mBytesPerFrame: 0, mChannelsPerFrame: 2, mBitsPerChannel: 0, mReserved: 0)
        var result: CMAudioFormatDescription?
        try #require(CMAudioFormatDescriptionCreate(allocator: nil, asbd: &asbd,
            layoutSize: 0, layout: nil, magicCookieSize: 0, magicCookie: nil,
            extensions: nil, formatDescriptionOut: &result) == noErr)
        return try #require(result)
    }
}
