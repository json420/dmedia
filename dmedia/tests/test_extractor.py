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

import os
from subprocess import CalledProcessError

from microfiber import random_id, Attachment

from .base import TempDir, SampleFilesTestCase, MagicLanternTestCase

from dmedia.importer import normalize_ext
from dmedia import extractor


# Known EXIF data as returned be exiftool:
sample_thm_exif = {
    'AEBAutoCancel': 'On',
    'AEBBracketValue': 0,
    'AEBSequence': '0,-,+',
    'AFAssistBeam': 'Emits',
    'AFMicroAdjMode': 'Disable',
    'AFMicroAdjValue': 0,
    'AFMicroadjustment': 'Disable; 0; 0; 0; 0',
    'AFOnAELockButtonSwitch': 'Disable',
    'AFPointAreaExpansion': 'Disable',
    'AFPointSelectionMethod': 'Normal',
    'AddOriginalDecisionData': 'Off',
    'Aperture': 11.0,
    'ApertureValue': 11.3,
    'Artist': '',
    'AspectRatio': '3:2',
    'AssignFuncButton': 'LCD brightness',
    'AudioBitrate': '1.54 Mbps',
    'AudioChannels': 2,
    'AudioSampleRate': 48000,
    'AutoExposureBracketing': 'Off',
    'AutoISO': 100,
    'AutoLightingOptimizer': 'Disable',
    'BaseISO': 100,
    'BitsPerSample': 8,
    'BlackMaskBottomBorder': 0,
    'BlackMaskLeftBorder': 0,
    'BlackMaskRightBorder': 0,
    'BlackMaskTopBorder': 0,
    'BracketMode': 'Off',
    'BracketShotNumber': 0,
    'BracketValue': 0,
    'BulbDuration': 0,
    'CameraTemperature': '30 C',
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
    'CropBottomMargin': 16,
    'CropLeftMargin': 24,
    'CropRightMargin': 24,
    'CropTopMargin': 16,
    'CroppedImageHeight': 1856,
    'CroppedImageLeft': 0,
    'CroppedImageTop': 0,
    'CroppedImageWidth': 2784,
    'CustomPictureStyleFileName': 'superflat01',
    'CustomRendered': 'Normal',
    'DateTimeOriginal': '2010:10:19 20:43:14',
    'DialDirectionTvAv': 'Normal',
    'DigitalGain': 0,
    'DigitalZoom': 'None',
    #'Directory': '/home/jderose/bzr/dmedia/py33/dmedia/tests/data',
    'DriveMode': 'Continuous Shooting',
    'Duration': '3.00 s',
    'EasyMode': 'Manual',
    'EncodingProcess': 'Baseline DCT, Huffman coding',
    'ExifByteOrder': 'Little-endian (Intel, II)',
    'ExifImageHeight': 120,
    'ExifImageWidth': 160,
    #'ExifToolVersion': 9.04,
    'ExifVersion': '0221',
    'ExposureCompensation': 0,
    'ExposureLevelIncrements': '1/3 Stop',
    'ExposureMode': 'Auto',
    'ExposureProgram': 'Manual',
    'ExposureTime': '1/100',
    'FNumber': 11.0,
    'FOV': '15.2 deg',
    #'FileAccessDate': '2013:02:21 03:21:46-07:00',
    #'FileModifyDate': '2013:02:21 03:20:50-07:00',
    #'FileName': 'MVI_5751.THM',
    #'FilePermissions': 'rw-rw-r--',
    #'FileSize': '27 kB',
    'FileType': 'THM',  # Utopic+
    #'FileType': 'JPEG',  # Trusty
    'Flash': 'Off, Did not fire',
    'FlashActivity': 0,
    'FlashBits': '(none)',
    'FlashExposureComp': 0,
    'FlashExposureLock': 'Off',
    'FlashGuideNumber': 0,
    'FlashSyncSpeedAv': 'Auto',
    'FlashpixVersion': '0100',
    'FocalLength': '138.0 mm',
    'FocalLength35efl': '138.0 mm (35 mm equivalent: 134.7 mm)',
    'FocalPlaneResolutionUnit': 'inches',
    'FocalPlaneXResolution': 109.6641535,
    'FocalPlaneYResolution': 125.2609603,
    'FocalUnits': '1/mm',
    'FocusDistanceLower': '1.57 m',  # Needed for Utopic+, not Trusty
    'FocusDistanceUpper': '1.64 m',  # Needed for Utopic+, not Trusty
    'FocusMode': 'Manual Focus (3)',
    'FocusRange': 'Not Known',
    'FocusingScreen': 'Eg-D',
    'FrameCount': 107,
    'FrameRate': 29.97,
    'GPSVersionID': '2.2.0.0',
    'HighISONoiseReduction': 'Standard',
    'HighlightTonePriority': 'Disable',
    'HyperfocalDistance': '56.23 m',
    'ISO': 100,
    'ISOExpansion': 'Off',
    'ISOSpeedIncrements': '1/3 Stop',
    'ImageHeight': 120,
    'ImageSize': '160x120',
    'ImageWidth': 160,
    'InternalSerialNumber': '',
    'InteropIndex': 'THM - DCF thumbnail file',
    'InteropVersion': '0100',
    'Lens': '70.0 - 200.0 mm',
    'Lens35efl': '70.0 - 200.0 mm (35 mm equivalent: 68.3 - 195.2 mm)',
    'LensAFStopButton': 'AF stop',
    'LensDriveNoAF': 'Focus search on',
    'LensID': 'Canon EF 70-200mm f/4L IS',
    'LensModel': 'EF70-200mm f/4L IS USM',
    'LensType': 'Canon EF 70-200mm f/4L IS',
    'LightValue': 13.6,
    'LiveViewShooting': 'On',
    'LongExposureNoiseReduction': 'Off',
    'LongExposureNoiseReduction2': 'Off',
    'MIMEType': 'image/jpeg',
    'MacroMode': 'Normal',
    'Make': 'Canon',
    'ManualFlashOutput': 'n/a',
    'MaxAperture': 4,
    'MaxFocalLength': '200 mm',
    'MeasuredEV': 12.5,
    'MeasuredEV2': 13,
    'MeteringMode': 'Center-weighted average',
    'MinAperture': 32,
    'MinFocalLength': '70 mm',
    'MirrorLockup': 'Disable',
    'Model': 'Canon EOS 5D Mark II',
    'ModifyDate': '2010:10:19 20:43:14',
    'NDFilter': 'n/a',
    'OpticalZoomCode': 'n/a',
    'Orientation': 'Horizontal (normal)',
    'OwnerName': '',
    'PictureStyle': 'User Def. 1',
    'Quality': 'n/a',
    'RawJpgSize': 'Large',
    'RecordMode': 'MOV',  # Utopic+
    #'RecordMode': 'Video',  # Trusty
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
    'SequenceNumber': 0,
    'SerialNumber': '0820500998',
    'SerialNumberFormat': 'Format 2',
    'SetButtonWhenShooting': 'Normal (disabled)',
    'Sharpness': 3,
    'SharpnessFrequency': 'n/a',
    'ShootingMode': 'Manual',
    'ShutterButtonAFOnButton': 'Metering + AF start',
    'ShutterSpeed': '1/100',
    'ShutterSpeedValue': '1/99',
    'SlowShutter': 'None',
    #'SourceFile': '/home/jderose/bzr/dmedia/py33/dmedia/tests/data/MVI_5751.THM',
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
    'VideoCodec': 'avc1',
    'WBBracketMode': 'Off',
    'WBBracketValueAB': 0,
    'WBBracketValueGM': 0,
    'WBShiftAB': 0,
    'WBShiftGM': 0,
    'WhiteBalance': 'Daylight',
    'WhiteBalanceBlue': 0,
    'WhiteBalanceRed': 0,
    'XResolution': 72,
    'YCbCrPositioning': 'Co-sited',
    'YCbCrSubSampling': 'YCbCr4:2:2 (2 1)',
    'YResolution': 72,
    'ZoomSourceWidth': 0,
    'ZoomTargetWidth': 0,
    # Added in Wily:
    'Megapixels': 0.019,
    'FileTypeExtension': 'thm',
    'DOF': '0.08 m (1.56 - 1.65 m)',
}



# exiftool adds some metadata that doesn't make sense to test
EXIFTOOL_IGNORE = (
    'Directory',
    'ExifToolVersion',
    'FileAccessDate',
    'FileModifyDate',
    'FileName',
    'FilePermissions',
    'FileSize',
    'SourceFile',
    'FileInodeChangeDate',
)


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
        self.assertEqual(exif, {})

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
                'height': 1080,
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

                'content_type': 'image/jpeg',
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
                'content_type': 'image/jpeg',
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

    def test_merge_exif(self):
        value1 = random_id()
        value2 = random_id()
        doc = {'foo': value1, 'meta': {'bar': value2}}
        self.assertIsNone(
            extractor.merge_exif(self.thm, doc, extractor.REMAP_EXIF)
        )
        self.assertEqual(
            doc,
            {
                'foo': value1,
                'ctime': 1287520994.68,
                'content_type': 'image/jpeg',
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

        value1 = random_id()
        value2 = random_id()
        doc = {'foo': value1, 'meta': {'bar': value2}}
        self.assertIsNone(
            extractor.merge_exif(self.thm, doc, extractor.REMAP_EXIF_THM)
        )
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

    def test_merge_mov_exif(self):
        value1 = random_id()
        value2 = random_id()
        doc = {'foo': value1, 'meta': {'bar': value2}}
        self.assertIsNone(
            extractor.merge_mov_exif(self.mov, doc)
        )
        self.assertEqual(
            doc,
            {
                'foo': value1,
                'ctime': 1287520994.68,
                'meta': {
                    'bar': value2,
                    'canon_thm': 'PW7537TPDOR78CYVL4NIASYTUVTEXJN5RUKYV5N7QNMLNBCT',
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

    def test_extract(self):
        # Test with sample MOV file from 5D Mark II:
        value1 = random_id()
        value2 = random_id()
        doc = {
            'ext': 'mov',
            'foo': value1,
            'meta': {'bar': value2},
        }
        self.assertIsNone(extractor.extract(self.mov, doc))
        self.assertEqual(
            doc,
            {
                'ext': 'mov',
                'foo': value1,
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
                'height': 1080,
                'samplerate': 48000, 
                'width': 1920,
                'ctime': 1287520994.68,
                'meta': {
                    'bar': value2,
                    'canon_thm': 'PW7537TPDOR78CYVL4NIASYTUVTEXJN5RUKYV5N7QNMLNBCT',
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
        self.assertIsInstance(t, Attachment)
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
        with self.assertRaises(CalledProcessError):
            t = extractor.thumbnail_video(invalid, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), ['invalid.mov'])

        # Test with non-existent file:
        tmp = TempDir()
        nope = tmp.join('nope.mov')
        with self.assertRaises(CalledProcessError):
            t = extractor.thumbnail_video(nope, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), [])

    def test_thumbnail_image(self):
        # Test with sample_thm from 5D Mark II:
        tmp = TempDir()
        t = extractor.thumbnail_image(self.thm, tmp.dir)
        self.assertIsInstance(t, Attachment)
        self.assertEqual(t.content_type, 'image/jpeg')
        self.assertIsInstance(t.data, bytes)
        self.assertGreater(len(t.data), 5000)
        self.assertEqual(os.listdir(tmp.dir), ['thumbnail.jpg'])

        # Test invalid file:
        tmp = TempDir()
        invalid = tmp.write(b'Wont work!', 'invalid.jpg')
        with self.assertRaises(CalledProcessError):
            t = extractor.thumbnail_image(invalid, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), ['invalid.jpg'])

        # Test with non-existent file:
        tmp = TempDir()
        nope = tmp.join('nope.jpg')
        with self.assertRaises(CalledProcessError):
            t = extractor.thumbnail_image(nope, tmp.dir)
        self.assertEqual(os.listdir(tmp.dir), [])

    def test_create_thumbnail(self):
        # Test with sample_mov from 5D Mark II:
        t = extractor.create_thumbnail(self.mov, 'mov')
        self.assertIsInstance(t, Attachment)
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

    def test_get_thumbnail_func(self):
        f = extractor.get_thumbnail_func
        self.assertIsNone(f({}))
        self.assertIsNone(f({'media': 'audio'}))
        self.assertIs(
            f({'media': 'video'}),
            extractor.thumbnail_video
        )
        self.assertIs(
            f({'media': 'image'}),
            extractor.thumbnail_image
        )
        self.assertIs(
            f({'media': 'image', 'ext': 'cr2'}),
            extractor.thumbnail_raw
        )

    def test_merge_thumbnail(self):
        # Test with sample_mov from 5D Mark II:
  
        doc = {
            '_attachments': {},
            'media': 'video',
            'ext': 'mov',
        }
        self.assertTrue(extractor.merge_thumbnail(self.mov, doc))
        self.assertEqual(set(doc['_attachments']), set(['thumbnail']))

        t = doc['_attachments']['thumbnail']
        self.assertIsInstance(t, dict)
        self.assertEqual(set(t), set(['content_type', 'data']))
        self.assertEqual(t['content_type'], 'image/jpeg')
        self.assertIsInstance(t['data'], str)
        self.assertGreater(len(t['data']), 5000)

        # Test when media is missing
        doc = {
            '_attachments': {},
            'ext': 'mov',
        }
        self.assertFalse(extractor.merge_thumbnail(self.mov, doc))
        self.assertEqual(doc,
            {
                '_attachments': {},
                'ext': 'mov',
            }
        )

        # Test when media is 'audio'
        doc = {
            '_attachments': {},
            'media': 'audio',
            'ext': 'mov',
        }
        self.assertFalse(extractor.merge_thumbnail(self.mov, doc))
        self.assertEqual(doc,
            {
                '_attachments': {},
                'media': 'audio',
                'ext': 'mov',
            }
        )

        # Test invalid file:
        tmp = TempDir()
        invalid = tmp.write(b'Wont work!', 'invalid.mov')
        doc = {
            '_attachments': {},
            'media': 'video',
            'ext': 'mov',
        }
        self.assertFalse(extractor.merge_thumbnail(invalid, doc))
        self.assertEqual(doc,
            {
                '_attachments': {},
                'media': 'video',
                'ext': 'mov',
            }
        )

        # Test with non-existent file:
        nope = tmp.join('nope.mov')
        doc = {
            '_attachments': {},
            'media': 'video',
            'ext': 'mov',
        }
        self.assertFalse(extractor.merge_thumbnail(nope, doc))
        self.assertEqual(doc,
            {
                '_attachments': {},
                'media': 'video',
                'ext': 'mov',
            }
        )        


NO_EXTRACT = (None, 'bin', 'bmp', 'cfg', 'dat', 'fir', 'log', 'lut', 'thm')

class TestMagicLantern(MagicLanternTestCase):
    """
    Test extractor with files from a typical Magic Lantern install.
    """

    def test_extract(self):
        extensions = set()
        for file in self.batch.files:
            ext = normalize_ext(file.name)
            extensions.add(ext)
            doc = {'ext': ext}
            self.assertIsNone(extractor.extract(file.name, doc))
            self.assertEqual(doc, {'ext': ext})
        self.assertEqual(
            extensions,
            set(NO_EXTRACT)
        )
  
