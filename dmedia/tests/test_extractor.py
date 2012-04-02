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
import os
from os import path
from subprocess import CalledProcessError

from microfiber import random_id

from .base import TempDir, SampleFilesTestCase

from dmedia import extractor


def b64decode(data):
    return base64.b64decode(data.encode('utf-8'))


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


# exiftool adds some metadata that doesn't make sense to test
EXIFTOOL_IGNORE = (
    'SourceFile',  # 'dmedia/tests/data/MVI_5751.THM'
    'ExifToolVersion',  # 8.15
    'FileName',  # 'MVI_5751.THM'
    'Directory',  # 'dmedia/tests/data',
    'FileSize',  # '27 kB'
    'FileModifyDate',  # '2010:10:19 20:43:18-06:00'
    'FilePermissions',  # 'rw-r--r--'
    'FileType',  # 'JPEG'
    'MIMEType',  # 'image/jpeg'
    'ExifByteOrder',  # 'Little-endian (Intel, II)'
)


# Known video info from dmedia-extract:
sample_mov_info = {
    "channels": 2, 
    "content_type": "video/quicktime", 
    "duration": {
        "frames": 107, 
        "nanoseconds": 3570233333, 
        "samples": 171371, 
        "seconds": 3.570233333
    },
    "framerate": {
        "denom": 1001, 
        "num": 30000
    }, 
    "media": "video",
    "height": 1088,  # FIXME: This is wrong, working around libavcodecs or GSTreamer bug!
    "samplerate": 48000, 
    "width": 1920
}


class TestFunctions(SampleFilesTestCase):

    maxDiff = None

    def test_raw_exiftool_extract(self):
        exif = extractor.raw_exiftool_extract(self.thm)
        for key in EXIFTOOL_IGNORE:
            exif.pop(key)
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
        exif = extractor.raw_exiftool_extract(jpg)
        for key in EXIFTOOL_IGNORE:
            exif.pop(key, None)
        self.assertEqual(exif, {'Error': 'File format error'})

        # Test with non-existent file:
        nope = tmp.join('nope.jpg')
        self.assertEqual(extractor.raw_exiftool_extract(nope), {})

    def test_raw_gst_extract(self):
        tmp = TempDir()

        # Test with sample MOV file from 5D Mark II:
        self.assertEqual(
            extractor.raw_gst_extract(self.mov),
            {
                'channels': 2, 
                'content_type': 'video/quicktime', 
                'duration': {
                    'frames': 107, 
                    'nanoseconds': 3570233333, 
                    'samples': 171371, 
                    'seconds': 3.570233333
                }, 
                'framerate': {
                    'denom': 1001, 
                    'num': 30000
                }, 
                'media': 'video',
                'height': 1088,  # FIXME: This is wrong, working around libavcodecs bug!
                'samplerate': 48000, 
                'width': 1920
            }
        )

        # Test with sample THM file from 5D Mark II:
        self.assertEqual(
            extractor.raw_gst_extract(self.thm),
            {
                'content_type': 'image/jpeg',
                'media': 'image',
                'height': 120,
                'width': 160,
            
            }
        )

        # Test invalid file:
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        self.assertEqual(extractor.raw_gst_extract(invalid), {})

        # Test with non-existent file:
        nope = tmp.join('nope.mov')
        self.assertEqual(extractor.raw_gst_extract(nope), {})

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

    def test_ctime_from_exif(self):
        f = extractor.ctime_from_exif
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

    def test_iter_exif(self):
        exif = extractor.raw_exiftool_extract(self.thm)
        self.assertEqual(
            dict(extractor.iter_exif(exif, extractor.REMAP_EXIF)),
            {
                'aperture': 11.0,
                'shutter': '1/100',
                'iso': 100,

                'camera_serial': '0820500998',
                'camera': 'Canon EOS 5D Mark II',
                'lens': 'Canon EF 70-200mm f/4L IS',
                'focal_length': '138.0 mm',

                'ctime': 1287520994.68,

                'height': 120,
                'width': 160,
            }
        )

        self.assertEqual(
            dict(extractor.iter_exif(exif, extractor.REMAP_EXIF_THM)),
            {
                'aperture': 11.0,
                'shutter': '1/100',
                'iso': 100,

                'camera_serial': '0820500998',
                'camera': 'Canon EOS 5D Mark II',
                'lens': 'Canon EF 70-200mm f/4L IS',
                'focal_length': '138.0 mm',

                'ctime': 1287520994.68,
            }
        )

    def test_merge_metadata(self):
        exif = extractor.raw_exiftool_extract(self.thm)

        items = tuple(extractor.iter_exif(exif, extractor.REMAP_EXIF))
        value1 = random_id()
        value2 = random_id()
        doc = {'foo': value1, 'meta': {'bar': value2}}
        self.assertIsNone(extractor.merge_metadata(doc, items))
        self.assertEqual(
            doc,
            {
                'foo': value1,
                'ctime': 1287520994.68,
                'height': 120,
                'width': 160,
                'meta': {
                    'bar': value2,
                    'aperture': 11.0,
                    'shutter': '1/100',
                    'iso': 100,
                    'camera_serial': '0820500998',
                    'camera': 'Canon EOS 5D Mark II',
                    'lens': 'Canon EF 70-200mm f/4L IS',
                    'focal_length': '138.0 mm',
                },
            }
        )

        items = tuple(extractor.iter_exif(exif, extractor.REMAP_EXIF_THM))
        value1 = random_id()
        value2 = random_id()
        doc = {'foo': value1, 'meta': {'bar': value2}}
        self.assertIsNone(extractor.merge_metadata(doc, items))
        self.assertEqual(
            doc,
            {
                'foo': value1,
                'ctime': 1287520994.68,
                'meta': {
                    'bar': value2,
                    'aperture': 11.0,
                    'shutter': '1/100',
                    'iso': 100,
                    'camera_serial': '0820500998',
                    'camera': 'Canon EOS 5D Mark II',
                    'lens': 'Canon EF 70-200mm f/4L IS',
                    'focal_length': '138.0 mm',
                },
            }
        )

    def test_thumbnail_video(self):
        # Test with sample_mov from 5D Mark II:
        tmp = TempDir()
        t = extractor.thumbnail_video(self.mov, tmp.dir)
        self.assertIsInstance(t, extractor.Thumbnail)
        self.assertEqual(t.content_type, 'image/jpeg')
        self.assertIsInstance(t.data, bytes)
        self.assertGreater(len(t.data), 5000)
        self.assertEqual(
            sorted(os.listdir(tmp.dir)),
            ['frame.png', 'thumbnail.jpg']
        )

        # Test invalid file:
        tmp = TempDir()
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        with self.assertRaises(CalledProcessError) as cm:
            t = extractor.thumbnail_video(invalid, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), ['invalid.mov'])

        # Test with non-existent file:
        tmp = TempDir()
        nope = tmp.join('nope.mov')
        with self.assertRaises(CalledProcessError) as cm:
            t = extractor.thumbnail_video(nope, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), [])

    def test_thumbnail_image(self):
        # Test with sample_thm from 5D Mark II:
        tmp = TempDir()
        t = extractor.thumbnail_image(self.thm, tmp.dir)
        self.assertIsInstance(t, extractor.Thumbnail)
        self.assertEqual(t.content_type, 'image/jpeg')
        self.assertIsInstance(t.data, bytes)
        self.assertGreater(len(t.data), 5000)
        self.assertEqual(os.listdir(tmp.dir), ['thumbnail.jpg'])

        # Test invalid file:
        tmp = TempDir()
        invalid = tmp.write(b'Wont work!', 'invalid.jpg')
        with self.assertRaises(CalledProcessError) as cm:
            t = extractor.thumbnail_image(invalid, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), ['invalid.jpg'])

        # Test with non-existent file:
        tmp = TempDir()
        nope = tmp.join('nope.jpg')
        with self.assertRaises(CalledProcessError) as cm:
            t = extractor.thumbnail_image(nope, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), [])

    def test_create_thumbnail(self):
        # Test with sample_mov from 5D Mark II:
        t = extractor.create_thumbnail(self.mov, 'mov')
        self.assertIsInstance(t, extractor.Thumbnail)
        self.assertEqual(t.content_type, 'image/jpeg')
        self.assertIsInstance(t.data, bytes)
        self.assertGreater(len(t.data), 5000)

        # Test when ext is None:
        self.assertIsNone(extractor.create_thumbnail(self.mov, None))

        # Test when ext is unknown
        self.assertIsNone(extractor.create_thumbnail(self.mov, 'nope'))
        
        # Test invalid file:
        tmp = TempDir()
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        self.assertIsNone(extractor.create_thumbnail(invalid, 'mov'))

        # Test with non-existent file:
        nope = tmp.join('nope.mov')
        self.assertIsNone(extractor.create_thumbnail(nope, 'mov'))
