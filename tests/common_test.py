#!/usr/bin/env python3

# backup-janitor
# Copyright (C) 2023  Dominic Rausch

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import unittest
from commands.common import parse_datetime
from datetime import datetime

class TestCommon(unittest.TestCase):

    def test_parsedatetime(self):
        # Invalid formats
        self.assertIsNone(parse_datetime(""))
        self.assertIsNone(parse_datetime(""))
        self.assertIsNone(parse_datetime("foo"))
        self.assertIsNone(parse_datetime("2022"))
        self.assertIsNone(parse_datetime("2022-05"))
        self.assertIsNone(parse_datetime("2022-02-15T"))

        # Good format
        self.assertEqual(parse_datetime("2022-02-15"), datetime(2022,2,15))
        self.assertEqual(parse_datetime("20220215"), datetime(2022,2,15))
        self.assertEqual(parse_datetime("2022_02_15"), datetime(2022,2,15))
        self.assertEqual(parse_datetime("2022-1-1"), datetime(2022,1,1))
        self.assertEqual(parse_datetime("2022-02-15T08"), datetime(2022,2,15,8))
        self.assertEqual(parse_datetime("2023-03-11T12_20"), datetime(2023,3,11,12,20))
        self.assertEqual(parse_datetime("2023-03-11T12-20-15"), datetime(2023,3,11,12,20,15))
        self.assertEqual(parse_datetime("2023-03-11_12-20"), datetime(2023,3,11,12,20))
        self.assertEqual(parse_datetime("20230311_1220"), datetime(2023,3,11,12,20))

        # Bad dates
        self.assertIsNone(parse_datetime("2022-15-02"))
        self.assertIsNone(parse_datetime("2023-03-11_12-60"))
        self.assertIsNone(parse_datetime("2023-02-29_11-11"))
