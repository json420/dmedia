# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Unit tests for `dmedia.extractor` module.
"""

import base64
from os import path

from .base import TempDir, SampleFilesTestCase

from dmedia import extractor

# Known EXIF data as returned be exiftool:
sample_thm_exif = {
    'AddOriginalDecisionData': 'Off',
    'AEBAutoCancel': 'On',
    'AEBBracketValue': 0,
    'AEBSequence': '0,-,+',
    'AFAssistBeam': 'Emits',
    'AFMicroAdjActive': 'No',
    'AFMicroadjustment': 'Disable; 0; 0; 0; 0',
    'AFMicroAdjValue': 0,
    'AFOnAELockButtonSwitch': 'Disable',
    'AFPointAreaExpansion': 'Disable',
    'AFPointSelectionMethod': 'Normal',
    'Aperture': 11.0,
    'ApertureValue': 11.300000000000001,
    'Artist': '',
    'AssignFuncButton': 'LCD brightness',
    'AutoExposureBracketing': 'Off',
    'AutoISO': 100,
    'AutoLightingOptimizer': 'Disable',
    'BaseISO': 100,
    'BlackMaskBottomBorder': 0,
    'BlackMaskLeftBorder': 0,
    'BlackMaskRightBorder': 0,
    'BlackMaskTopBorder': 0,
    'BracketMode': 'Off',
    'BracketShotNumber': 0,
    'BracketValue': 0,
    'BulbDuration': 0,
    'CameraType': 'EOS High-end',
    'CanonExposureMode': 'Manual',
    'CanonFirmwareVersion': 'Firmware Version 2.0.7',
    'CanonFlashMode': 'Off',
    'CanonImageSize': '1920x1080 Movie',
    'CanonImageType': 'MVI:Canon EOS 5D Mark II',
    'CanonModelID': 'EOS 5D Mark II',
    'CircleOfConfusion': '0.031 mm',
    'ColorComponents': 3,
    'ColorSpace': 'sRGB',
    'ColorTemperature': 3600,
    'ColorTone': 'Normal',
    'ComponentsConfiguration': 'Y, Cb, Cr, -',
    'ContinuousDrive': 'Movie',
    'Contrast': -4,
    'ControlMode': 'Camera Local Control',
    'Copyright': '',
    'CreateDate': '2010:10:19 20:43:14',    
    'CustomRendered': 'Normal',
    'DateTimeOriginal': '2010:10:19 20:43:14',
    'DialDirectionTvAv': 'Normal',
    'DigitalGain': 0,
    'DigitalZoom': 'None',
    #'Directory': 'dmedia/tests/data',
    'DriveMode': 'Continuous Shooting',
    'EasyMode': 'Manual',
    'EncodingProcess': 'Baseline DCT, Huffman coding',
    #'ExifByteOrder': 'Little-endian (Intel, II)',
    'ExifImageHeight': 120,
    'ExifImageWidth': 160,
    #'ExifToolVersion': 8.1500000000000004,
    'ExifVersion': '0221',
    'ExposureCompensation': 0,
    'ExposureLevelIncrements': '1/3 Stop',
    'ExposureMode': 'Auto',
    'ExposureProgram': 'Manual',
    'ExposureTime': '1/100',
    #'FileModifyDate': '2010:10:19 20:43:18-06:00',
    #'FileName': 'MVI_5751.THM',
    #'FilePermissions': 'rw-r--r--',
    #'FileSize': '27 kB',
    #'FileType': 'JPEG',
    'FlashActivity': 0,
    'FlashBits': '(none)',
    'FlashExposureComp': 0, 'SequenceNumber': 0,
    'FlashExposureLock': 'Off',
    'FlashGuideNumber': 0,
    'FlashpixVersion': '0100',
    'FlashSyncSpeedAv': 'Auto',
    'Flash': 'Off, Did not fire',
    'FNumber': 11.0,
    'FocalLength35efl': '138.0 mm (35 mm equivalent: 134.7 mm)',
    'FocalLength': '138.0 mm',
    'FocalPlaneResolutionUnit': 'inches',
    'FocalPlaneXResolution': 109.6641535,
    'FocalPlaneYResolution': 125.26096029999999,
    'FocalUnits': '1/mm',
    'FocusingScreen': 'Eg-D',
    'FocusMode': 'Manual Focus (3)',
    'FocusRange': 'Not Known',
    'FOV': '15.2 deg',
    'GPSVersionID': '2.2.0.0',
    'HighISONoiseReduction': 'Standard',
    'HighlightTonePriority': 'Disable',
    'HyperfocalDistance': '56.23 m',
    'ImageHeight': 120,
    'ImageSize': '160x120',
    'ImageWidth': 160,
    'InternalSerialNumber': '',
    'InteropIndex': 'THM - DCF thumbnail file',
    'InteropVersion': '0100',
    'ISO': 100,
    'ISOExpansion': 'Off',
    'ISOSpeedIncrements': '1/3 Stop',
    'Lens35efl': '70.0 - 200.0 mm (35 mm equivalent: 68.3 - 195.2 mm)',
    'LensAFStopButton': 'AF stop',
    'LensDriveNoAF': 'Focus search on',
    'LensID': 'Canon EF 70-200mm f/4L IS',
    'LensModel': 'EF70-200mm f/4L IS USM',
    'LensType': 'Canon EF 70-200mm f/4L IS',
    'Lens': '70.0 - 200.0 mm',
    'LightValue': 13.6,
    'LiveViewShooting': 'On',
    'LongExposureNoiseReduction2': 'Off',
    'LongExposureNoiseReduction': 'Off',
    'LongFocal': '200 mm',
    'MacroMode': 'Normal',
    'Make': 'Canon',
    'ManualFlashOutput': 'n/a',
    'MaxAperture': 4,
    'MeasuredEV': 12.5,
    'MeasuredEV2': 13,
    'MeteringMode': 'Center-weighted average',
    #'MIMEType': 'image/jpeg',
    'MinAperture': 32,
    'MirrorLockup': 'Disable',
    'Model': 'Canon EOS 5D Mark II',
    'ModifyDate': '2010:10:19 20:43:14',
    'NDFilter': 'n/a',
    'OpticalZoomCode': 'n/a',
    'Orientation': 'Horizontal (normal)',
    'OwnerName': '',
    'PictureStyle': 'User Def. 1',
    'Quality': 'Unknown (-1)',
    'RawJpgSize': 'Large',
    'RecordMode': 'Video',
    'RelatedImageHeight': 1080,
    'RelatedImageWidth': 1920,
    'ResolutionUnit': 'inches',
    'SafetyShift': 'Disable',
    'Saturation': 'Normal',
    'ScaleFactor35efl': 1.0,
    'SceneCaptureType': 'Standard',
    'SelfTimer': 'Off',
    'SensorBlueLevel': 0,
    'SensorBottomBorder': 3799,
    'SensorHeight': 3804,
    'SensorLeftBorder': 168,
    'SensorRedLevel': 0,
    'SensorRightBorder': 5783,
    'SensorTopBorder': 56,
    'SensorWidth': 5792,
    'SerialNumberFormat': 'Format 2',
    'SerialNumber': '0820500998',
    'SetButtonWhenShooting': 'Normal (disabled)',
    'Sharpness': 3,
    'SharpnessFrequency': 'n/a',
    'ShootingMode': 'Manual',
    'ShortFocal': '70 mm',
    'ShutterButtonAFOnButton': 'Metering + AF start',
    'ShutterSpeed': '1/100',
    'ShutterSpeedValue': '1/99',
    'SlowShutter': 'None',
    #'SourceFile': 'dmedia/tests/data/MVI_5751.THM',
    'SubSecCreateDate': '2010:10:19 20:43:14.68',
    'SubSecDateTimeOriginal': '2010:10:19 20:43:14.68',
    'SubSecModifyDate': '2010:10:19 20:43:14.68',
    'SubSecTime': 68,
    'SubSecTimeDigitized': 68,
    'SubSecTimeOriginal': 68,
    'SuperimposedDisplay': 'On',
    'TargetAperture': 11,
    'TargetExposureTime': '1/102',
    'ThumbnailImageValidArea': '0 159 15 104',
    'ToneCurve': 'Standard',
    'UserComment': '',
    'VRDOffset': 0,
    #'Warning': 'Invalid CanonAFInfo2 data',  Not present under Oneiric
    'WBBracketMode': 'Off',
    'WBBracketValueAB': 0,
    'WBBracketValueGM': 0,
    'WBShiftAB': 0,
    'WBShiftGM': 0,
    'WhiteBalanceBlue': 0,
    'WhiteBalanceRed': 0,
    'WhiteBalance': 'Daylight',
    'XResolution': 72,
    'YCbCrPositioning': 'Co-sited',
    'YCbCrSubSampling': 'YCbCr4:2:2 (2 1)',
    'YResolution': 72,
    'ZoomSourceWidth': 0,
    'ZoomTargetWidth': 0,
    'BitsPerSample': 8,
}

# These values are new running on Oneiric
sample_thm_exif2 = {
    'CropLeftMargin': 24,
    'CropRightMargin': 24,
    'CropTopMargin': 16,
    'CropBottomMargin': 16,
    
    'CroppedImageWidth': 2784,
    'CroppedImageHeight': 1856,
    
    'VideoCodec': 'avc1',

    'AudioBitrate': '1.54 Mbps',
    'CustomPictureStyleFileName': 'superflat01', 
    'Duration': '3.00 s',
    'FrameRate': 29.97, 

    'AudioChannels': 2,
    'AudioSampleRate': 48000,
    'CameraTemperature': '30 C',

    'AspectRatio': '3:2',

    'FrameCount': 107,
}

sample_thm_exif.update(sample_thm_exif2)


# Known video info from totem-video-indexer:
sample_mov_info = {
    'TOTEM_INFO_DURATION': '3',
    'TOTEM_INFO_HAS_VIDEO': 'True',
    'TOTEM_INFO_VIDEO_WIDTH': '1920',
    'TOTEM_INFO_VIDEO_HEIGHT': '1080',
    'TOTEM_INFO_VIDEO_CODEC': 'H.264 / AVC',
    'TOTEM_INFO_FPS': '30',
    'TOTEM_INFO_HAS_AUDIO': 'True',
    'TOTEM_INFO_AUDIO_CODEC': 'Raw 16-bit PCM audio',
    'TOTEM_INFO_AUDIO_SAMPLE_RATE': '48000',
    'TOTEM_INFO_AUDIO_CHANNELS': 'Stereo',
}


class TestFunctions(SampleFilesTestCase):

    def test_file_2_base64(self):
        f = extractor.file_2_base64
        tmp = TempDir()
        src = tmp.write(b'Hello naughty nurse!', 'sample.txt')
        self.assertEqual(
            base64.b64decode(f(src)),
            b'Hello naughty nurse!'
        )


    def test_extract_exif(self):
        f = extractor.extract_exif
        exif = f(self.thm)
        self.assertEqual(set(sample_thm_exif), set(exif))
        for key in sample_thm_exif:
            v1 = sample_thm_exif[key]
            v2 = exif[key]
            self.assertEqual(v1, v2, '{!r}: {!r} != {!r}'.format(key, v1, v2))
        self.assertEqual(sample_thm_exif, exif)

        # Test that error is returned for invalid file:
        tmp = TempDir()
        data = b'Foo Bar\n' * 1000
        jpg = tmp.write(data, 'sample.jpg')
        self.assertEqual(
            f(jpg),
            {'Error': 'File format error'}
        )

        # Test with non-existent file:
        nope = tmp.join('nope.jpg')
        self.assertEqual(f(nope), {})

    def test_parse_subsec_datetime(self):
        f = extractor.parse_subsec_datetime

        # Test with wrong type:
        self.assertEqual(f(None), None)
        self.assertEqual(f(17), None)

        # Test with multiple periods:
        self.assertEqual(f('2010:10:21.01:44:37.40'), None)

        # Test with incorrect datetime length:
        self.assertEqual(f('2010:10:21  01:44:37.40'), None)
        self.assertEqual(f('2010:10:2101:44:37.40'), None)
        self.assertEqual(f('2010:10:21  01:44:37'), None)
        self.assertEqual(f('2010:10:2101:44:37'), None)

        # Test with nonesense datetime:
        self.assertEqual(f('2010:80:21 01:44:37.40'), None)
        self.assertEqual(f('2010:80:21 01:44:37'), None)

        # Test with incorrect subsec length:
        self.assertEqual(f('2010:10:21 01:44:37.404'), None)
        self.assertEqual(f('2010:10:21 01:44:37.4'), None)

        # Test with negative subsec:
        self.assertEqual(f('2010:10:21 01:44:37.-4'), None)

        # Test with nonsense subsec:
        self.assertEqual(f('2010:10:21 01:44:37.AB'), None)

        # Test with valid timestamps:
        self.assertEqual(
            f('2010:10:21 01:44:37.40'),
            (1287625477 + 40 / 100.0)
        )
        self.assertEqual(f('2010:10:21 01:44:37'), 1287625477)


    def test_extract_mtime_from_exif(self):
        f = extractor.extract_mtime_from_exif
        self.assertEqual(
            f(sample_thm_exif),
            (1287520994 + 68 / 100.0)
        )
        d = dict(sample_thm_exif)
        del d['SubSecCreateDate']
        self.assertEqual(f(d), 1287520994 + 68 / 100.0)
        del d['SubSecDateTimeOriginal']
        self.assertEqual(f(d), 1287520994 + 68 / 100.0)
        del d['SubSecModifyDate']
        self.assertEqual(f(d), None)


    def test_extract_video_info(self):
        f = extractor.extract_video_info
        tmp = TempDir()

        # Test with sample_mov from 5D Mark II:
        info = f(self.mov)
        self.assertEqual(sample_mov_info, info)

        # Test invalid file:
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        self.assertEqual(
            f(invalid),
            {
                'TOTEM_INFO_HAS_VIDEO': 'False',
                'TOTEM_INFO_HAS_AUDIO': 'False',
            }
        )

        # Test with non-existent file:
        nope = tmp.join('nope.mov')
        self.assertEqual(
            f(nope),
            {
                'TOTEM_INFO_HAS_VIDEO': 'False',
                'TOTEM_INFO_HAS_AUDIO': 'False',
            }
        )


    def test_generate_thumbnail(self):
        f = extractor.generate_thumbnail
        tmp = TempDir()

        # Test with sample_mov from 5D Mark II:
        d = f(self.mov)
        self.assertTrue(isinstance(d, dict))
        self.assertEqual(sorted(d), ['content_type', 'data'])
        self.assertEqual(d['content_type'], 'image/jpeg')
        data = base64.b64decode(d['data'])

        # Test invalid file:
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        self.assertEqual(f(invalid), None)

        # Test with non-existent file:
        nope = tmp.join('nope.mov')
        self.assertEqual(f(nope), None)


    def test_merge_metadata(self):
        f = extractor.merge_metadata
        tmp = TempDir()

        doc = dict(ext='mov')
        f(self.mov, doc)

        # Check canon.thm attachment
        att = doc.pop('_attachments')
        self.assertEqual(set(att), set(['canon.thm', 'thumbnail']))
        self.assertEqual(set(att['canon.thm']), set(['content_type', 'data']))
        self.assertEqual(att['canon.thm']['content_type'], 'image/jpeg')
        self.assertEqual(
            base64.b64decode(att['canon.thm']['data']),
            open(self.thm, 'rb').read()
        )

        # Check thumbnail
        thm = att['thumbnail']
        self.assertTrue(isinstance(thm, dict))
        self.assertEqual(sorted(thm), ['content_type', 'data'])
        self.assertEqual(thm['content_type'], 'image/jpeg')
        data = base64.b64decode(thm['data'])

        self.assertEqual(
            doc,
            dict(
                ext='mov',
                mtime=1287520994 + 68 / 100.0,
                meta=dict(
                    width=1920,
                    height=1080,
                    duration=3,
                    codec_video='H.264 / AVC',
                    codec_audio='Raw 16-bit PCM audio',
                    sample_rate=48000,
                    fps=30,
                    channels='Stereo',
                    iso=100,
                    shutter='1/100',
                    aperture=11.0,
                    lens='Canon EF 70-200mm f/4L IS',
                    camera='Canon EOS 5D Mark II',
                    camera_serial='0820500998',
                    focal_length='138.0 mm',
                ),
            )
        )

    def test_merge_exif(self):
        f = extractor.merge_exif
        self.assertTrue(self.thm.endswith('.THM'))
        attachments = {}
        self.assertEqual(
            dict(f(self.thm, attachments)),
            dict(
                width=160,
                height=120,
                iso=100,
                shutter='1/100',
                aperture=11.0,
                lens='Canon EF 70-200mm f/4L IS',
                camera='Canon EOS 5D Mark II',
                camera_serial='0820500998',
                focal_length='138.0 mm',
                mtime=1287520994 + 68 / 100.0,
            ),
        )
        self.assertEqual(attachments, {})


    def test_merge_video_info(self):
        f = extractor.merge_video_info
        tmp = TempDir()

        att = {}
        merged = dict(f(self.mov, att))

        # Check canon.thm attachment
        self.assertEqual(set(att), set(['thumbnail', 'canon.thm']))
        self.assertEqual(set(att['canon.thm']), set(['content_type', 'data']))
        self.assertEqual(att['canon.thm']['content_type'], 'image/jpeg')
        self.assertEqual(
            base64.b64decode(att['canon.thm']['data']),
            open(self.thm, 'rb').read()
        )

        # Check thumbnail
        thm = att['thumbnail']
        self.assertTrue(isinstance(thm, dict))
        self.assertEqual(sorted(thm), ['content_type', 'data'])
        self.assertEqual(thm['content_type'], 'image/jpeg')
        data = base64.b64decode(thm['data'])

        self.assertEqual(
            merged,
            dict(
                width=1920,
                height=1080,
                duration=3,
                codec_video='H.264 / AVC',
                codec_audio='Raw 16-bit PCM audio',
                sample_rate=48000,
                fps=30,
                channels='Stereo',
                iso=100,
                shutter='1/100',
                aperture=11.0,
                lens='Canon EF 70-200mm f/4L IS',
                camera='Canon EOS 5D Mark II',
                camera_serial='0820500998',
                focal_length='138.0 mm',
                mtime=1287520994 + 68 / 100.0,
            )
        )

        # Test invalid file:
        invalid_mov = tmp.write(b'Wont work!', 'invalid.mov')
        invalid_thm = tmp.write(b'Wont work either!', 'invalid.thm')
        att = {}
        merged = dict(f(invalid_mov, att))
        self.assertEqual(merged, {})
        self.assertEqual(att, {})
