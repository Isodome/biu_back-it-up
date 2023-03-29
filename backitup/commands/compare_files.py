import os
import stat
from collections import namedtuple

BUFFER_SIZE = 10*1024


def read_whole_file(path):
    with path.open('rb') as f:
        return f.read()


def file_contents_identical(lhs, rhs, lhs_bytes=None):
    with lhs.open('rb') as f_lhs, rhs.open('rb') as f_rhs:
        while True:
            buf_lhs = f_lhs.read(BUFFER_SIZE)
            buf_rhs = f_rhs.read(BUFFER_SIZE)
            if not buf_lhs:
                return not buf_rhs
            if buf_lhs != buf_rhs:
                return False


def file_bytes_are(bytes, path):
    with path.open('rb') as f:
        return bytes == f.read(BUFFER_SIZE) and not f.read(1)


DedupResult = namedtuple("DedupResult", "dups no_dups")


def list_duplicate_of(existing_file: str, candidate_dups):
    if not (existing_file and candidate_dups):
        return DedupResult(dups=[], no_dups=candidate_dups)

    stat_ex = existing_file.stat()
    if not stat.S_ISREG(stat_ex.st_mode):
        return DedupResult(dups=[], no_dups=candidate_dups)

    buffer = None
    if len(candidate_dups) > 1 and stat_ex.st_size <= BUFFER_SIZE:
        buffer = read_whole_file(existing_file)

    no_dups = []
    dups = []
    for candidate_dup in candidate_dups:
        identical = file_bytes_are(buffer, candidate_dup) if buffer else file_contents_identical(
            existing_file, candidate_dup)
        if identical:
            dups.append(candidate_dup)
        else:
            no_dups.append(candidate_dup)
    return DedupResult(dups=dups, no_dups=no_dups)


def group_duplicates(candidate_dups):
    if not candidate_dups:
        return []
    result = []
    no_dups = candidate_dups

    while no_dups:
        cand = no_dups[0]
        dups, no_dups = list_duplicate_of(cand, no_dups[1:])
        dups.append(cand)
        result.append(dups)
    return result
