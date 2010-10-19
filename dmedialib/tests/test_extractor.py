# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.


"""
Unit tests for `dmedialib.extractor` module.
"""

import base64
import Image
from .helpers import sample_mov, sample_thm, assert_deepequal, TempDir
from dmedialib import extractor

# Known EXIF data as returned be exiftool:
sample_thm_exif = {
    u'AddOriginalDecisionData': u'Off',
    u'AEBAutoCancel': u'On',
    u'AEBBracketValue': 0,
    u'AEBSequence': u'0,-,+',
    u'AFAssistBeam': u'Emits',
    u'AFMicroAdjActive': u'No',
    u'AFMicroadjustment': u'Disable; 0; 0; 0; 0',
    u'AFMicroAdjValue': 0,
    u'AFOnAELockButtonSwitch': u'Disable',
    u'AFPointAreaExpansion': u'Disable',
    u'AFPointSelectionMethod': u'Normal',
    u'Aperture': 11.0,
    u'ApertureValue': 11.300000000000001,
    u'Artist': u'',
    u'AssignFuncButton': u'LCD brightness',
    u'AutoExposureBracketing': u'Off',
    u'AutoISO': 100,
    u'AutoLightingOptimizer': u'Disable',
    u'BaseISO': 100,
    u'BlackMaskBottomBorder': 0,
    u'BlackMaskLeftBorder': 0,
    u'BlackMaskRightBorder': 0,
    u'BlackMaskTopBorder': 0,
    u'BracketMode': u'Off',
    u'BracketShotNumber': 0,
    u'BracketValue': 0,
    u'BulbDuration': 0,
    u'CameraType': u'EOS High-end',
    u'CanonExposureMode': u'Manual',
    u'CanonFirmwareVersion': u'Firmware Version 2.0.7',
    u'CanonFlashMode': u'Off',
    u'CanonImageSize': u'Unknown (142)',
    u'CanonImageType': u'MVI:Canon EOS 5D Mark II',
    u'CanonModelID': u'EOS 5D Mark II',
    u'CircleOfConfusion': u'0.031 mm',
    u'ColorComponents': 3,
    u'ColorSpace': u'sRGB',
    u'ColorTemperature': 3600,
    u'ColorTone': u'Normal',
    u'ComponentsConfiguration': u'Y, Cb, Cr, -',
    u'ContinuousDrive': u'Movie',
    u'Contrast': -4,
    u'ControlMode': u'Camera Local Control',
    u'Copyright': u'',
    u'CreateDate': u'2010:10:19 20:43:14',
    u'CustomRendered': u'Normal',
    u'DateTimeOriginal': u'2010:10:19 20:43:14',
    u'DialDirectionTvAv': u'Normal',
    u'DigitalGain': 0,
    u'DigitalZoom': u'None',
    #u'Directory': u'/home/jderose/bzr/dmedia/dmedialib/tests/data',
    u'DriveMode': u'Continuous shooting',
    u'EasyMode': u'Manual',
    u'EncodingProcess': u'Baseline DCT, Huffman coding',
    u'ExifByteOrder': u'Little-endian (Intel, II)',
    u'ExifImageHeight': 120,
    u'ExifImageWidth': 160,
    #u'ExifToolVersion': 8.1500000000000004,
    u'ExifVersion': u'0221',
    u'ExposureCompensation': 0,
    u'ExposureLevelIncrements': u'1/3 Stop',
    u'ExposureMode': u'Auto',
    u'ExposureProgram': u'Manual',
    u'ExposureTime': u'1/100',
    #u'FileModifyDate': u'2010:10:19 20:43:18-06:00',
    #u'FileName': u'MVI_5751.THM',
    #u'FilePermissions': u'rw-r--r--',
    #u'FileSize': u'27 kB',
    #u'FileType': u'JPEG',
    u'FlashActivity': 0,
    u'FlashBits': u'(none)',
    u'FlashExposureComp': 0, u'SequenceNumber': 0,
    u'FlashExposureLock': u'Off',
    u'FlashGuideNumber': 0,
    u'FlashpixVersion': u'0100',
    u'FlashSyncSpeedAv': u'Auto',
    u'Flash': u'Off, Did not fire',
    u'FNumber': 11.0,
    u'FocalLength35efl': u'138.0 mm (35 mm equivalent: 134.7 mm)',
    u'FocalLength': u'138.0 mm',
    u'FocalPlaneResolutionUnit': u'inches',
    u'FocalPlaneXResolution': 109.6641535,
    u'FocalPlaneYResolution': 125.26096029999999,
    u'FocalUnits': u'1/mm',
    u'FocusingScreen': u'Eg-D',
    u'FocusMode': u'Manual Focus (3)',
    u'FocusRange': u'Not Known',
    u'FOV': u'15.2 deg',
    u'GPSVersionID': u'2.2.0.0',
    u'HighISONoiseReduction': u'Standard',
    u'HighlightTonePriority': u'Disable',
    u'HyperfocalDistance': u'56.23 m',
    u'ImageHeight': 120,
    u'ImageSize': u'160x120',
    u'ImageWidth': 160,
    u'InternalSerialNumber': u'',
    u'InteropIndex': u'THM - DCF thumbnail file',
    u'InteropVersion': u'0100',
    u'ISO': 100,
    u'ISOExpansion': u'Off',
    u'ISOSpeedIncrements': u'1/3 Stop',
    u'Lens35efl': u'70.0 - 200.0 mm (35 mm equivalent: 68.3 - 195.2 mm)',
    u'LensAFStopButton': u'AF stop',
    u'LensDriveNoAF': u'Focus search on',
    u'LensID': u'Canon EF 70-200mm f/4L IS',
    u'LensModel': u'EF70-200mm f/4L IS USM',
    u'LensType': u'Canon EF 70-200mm f/4L IS',
    u'Lens': u'70.0 - 200.0 mm',
    u'LightValue': 13.6,
    u'LiveViewShooting': u'On',
    u'LongExposureNoiseReduction2': u'Off',
    u'LongExposureNoiseReduction': u'Off',
    u'LongFocal': u'200 mm',
    u'MacroMode': u'Normal',
    u'Make': u'Canon',
    u'ManualFlashOutput': u'n/a',
    u'MaxAperture': 4,
    u'MeasuredEV': 12.5,
    u'MeasuredEV2': 13,
    u'MeteringMode': u'Center-weighted average',
    u'MIMEType': u'image/jpeg',
    u'MinAperture': 32,
    u'MirrorLockup': u'Disable',
    u'Model': u'Canon EOS 5D Mark II',
    u'ModifyDate': u'2010:10:19 20:43:14',
    u'NDFilter': u'Unknown (-1)',
    u'OpticalZoomCode': u'n/a',
    u'Orientation': u'Horizontal (normal)',
    u'OwnerName': u'',
    u'PictureStyle': u'User Def. 1',
    u'Quality': u'Unknown (-1)',
    u'RawJpgSize': u'Large',
    u'RecordMode': u'Unknown (9)',
    u'RelatedImageHeight': 1080,
    u'RelatedImageWidth': 1920,
    u'ResolutionUnit': u'inches',
    u'SafetyShift': u'Disable',
    u'Saturation': u'Normal',
    u'ScaleFactor35efl': 1.0,
    u'SceneCaptureType': u'Standard',
    u'SelfTimer': u'Off',
    u'SensorBlueLevel': 0,
    u'SensorBottomBorder': 3799,
    u'SensorHeight': 3804,
    u'SensorLeftBorder': 168,
    u'SensorRedLevel': 0,
    u'SensorRightBorder': 5783,
    u'SensorTopBorder': 56,
    u'SensorWidth': 5792,
    u'SerialNumberFormat': u'Format 2',
    u'SerialNumber': u'0820500998',
    u'SetButtonWhenShooting': u'Normal (disabled)',
    u'Sharpness': 3,
    u'SharpnessFrequency': u'n/a',
    u'ShootingMode': u'Manual',
    u'ShortFocal': u'70 mm',
    u'ShutterButtonAFOnButton': u'Metering + AF start',
    u'ShutterSpeed': u'1/100',
    u'ShutterSpeedValue': u'1/99',
    u'SlowShutter': u'None',
    #u'SourceFile': u'/home/jderose/bzr/dmedia/dmedialib/tests/data/MVI_5751.THM',
    u'SubSecCreateDate': u'2010:10:19 20:43:14.68',
    u'SubSecDateTimeOriginal': u'2010:10:19 20:43:14.68',
    u'SubSecModifyDate': u'2010:10:19 20:43:14.68',
    u'SubSecTime': 68,
    u'SubSecTimeDigitized': 68,
    u'SubSecTimeOriginal': 68,
    u'SuperimposedDisplay': u'On',
    u'TargetAperture': 11,
    u'TargetExposureTime': u'1/102',
    u'ThumbnailImageValidArea': u'0 159 15 104',
    u'ToneCurve': u'Standard',
    u'UserComment': u'',
    u'VRDOffset': 0,
    u'Warning': u'Invalid CanonAFInfo2 data',
    u'WBBracketMode': u'Off',
    u'WBBracketValueAB': 0,
    u'WBBracketValueGM': 0,
    u'WBShiftAB': 0,
    u'WBShiftGM': 0,
    u'WhiteBalanceBlue': 0,
    u'WhiteBalanceRed': 0,
    u'WhiteBalance': u'Daylight',
    u'XResolution': 72,
    u'YCbCrPositioning': u'Co-sited',
    u'YCbCrSubSampling': u'YCbCr4:2:2 (2 1)',
    u'YResolution': 72,
    u'ZoomSourceWidth': 0,
    u'ZoomTargetWidth': 0, u'BitsPerSample': 8,
}

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


def test_file_2_base64():
    f = extractor.file_2_base64
    tmp = TempDir()
    src = tmp.write('Hello naughty nurse!', 'sample.txt')
    assert base64.b64decode(f(src)) == 'Hello naughty nurse!'


def test_extract_exif():
    f = extractor.extract_exif
    exif = f(sample_thm)
    assert_deepequal(sample_thm_exif, exif)

    # Test that error is returned for invalid file:
    tmp = TempDir()
    data = 'Foo Bar\n' * 1000
    jpg = tmp.write(data, 'sample.jpg')
    assert f(jpg) == {u'Error': u'File format error'}

    # Test with non-existent file:
    nope = tmp.join('nope.jpg')
    assert f(nope) == {u'Error': u'ValueError: No JSON object could be decoded'}


def test_extract_video_info():
    f = extractor.extract_video_info
    tmp = TempDir()

    # Test with sample_mov from 5D Mark II:
    info = f(sample_mov)
    assert_deepequal(sample_mov_info, info)

    # Test invalid file:
    invalid = tmp.write('Wont work!', 'invalid.mov')
    assert f(invalid) == {
        'TOTEM_INFO_HAS_VIDEO': 'False',
        'TOTEM_INFO_HAS_AUDIO': 'False',
    }

    # Test with non-existent file:
    nope = tmp.join('nope.mov')
    assert f(nope) == {
        'TOTEM_INFO_HAS_VIDEO': 'False',
        'TOTEM_INFO_HAS_AUDIO': 'False',
    }


def test_generate_thumbnail():
    f = extractor.generate_thumbnail
    tmp = TempDir()

    # Test with sample_mov from 5D Mark II:
    d = f(sample_mov)
    assert isinstance(d, dict)
    assert sorted(d) == ['content_type', 'data']
    assert d['content_type'] == 'image/jpeg'
    data = base64.b64decode(d['data'])
    jpg = tmp.write(data, 'thumbnail.jpg')
    assert Image.open(jpg).size == (192, 108)

    # Test invalid file:
    invalid = tmp.write('Wont work!', 'invalid.mov')
    assert f(invalid) is None

    # Test with non-existent file:
    nope = tmp.join('nope.mov')
    assert f(nope) is None
