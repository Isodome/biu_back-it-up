#!/usr/bin/env python3

# biu - back it up!
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


from datetime import datetime

from backitup.backups.backup import parse_datetime


class TestCommon:
    def test_parsedatetime(self):
        # Invalid formats
        assert parse_datetime("") is None
        assert parse_datetime("") is None
        assert parse_datetime("foo") is None
        assert parse_datetime("2022") is None
        assert parse_datetime("2022-05") is None
        assert parse_datetime("2022-02-15T") is None

        # Good format
        assert parse_datetime("2022-02-15") == datetime(2022, 2, 15)
        assert parse_datetime("20220215") == datetime(2022, 2, 15)
        assert parse_datetime("2022_02_15") == datetime(2022, 2, 15)
        assert parse_datetime("2022-02-15T08") == datetime(2022, 2, 15, 8)
        assert parse_datetime(
            "2023-03-11T12_20") == datetime(2023, 3, 11, 12, 20)
        assert parse_datetime(
            "2023-03-11T12-20-15") == datetime(2023, 3, 11, 12, 20, 15)
        assert parse_datetime(
            "2023-03-11_12-20") == datetime(2023, 3, 11, 12, 20)
        assert parse_datetime("20230311_1220") == datetime(2023, 3, 11, 12, 20)

        # Bad dates
        assert parse_datetime("2022-15-02") is None
        assert parse_datetime("2023-03-11_12-60") is None
        assert parse_datetime("2023-02-29_11-11") is None
        assert parse_datetime("2022-1-1") is None
