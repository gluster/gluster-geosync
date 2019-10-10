import os
import logging
from ctypes import CDLL, RTLD_GLOBAL, get_errno, byref, c_ulong
from ctypes.util import find_library
from ctypes import create_string_buffer

from glustergeosync.syncutils import logf

libgfc = CDLL(
    find_library("gfchangelog"),
    mode=RTLD_GLOBAL,
    use_errno=True
)


class ChangelogHistoryNotAvailable(Exception):
    pass


class ChangelogException(OSError):
    pass


def _raise_changelog_err():
    errn = get_errno()
    raise ChangelogException(errn, os.strerror(errn))


def _init():
    if libgfc.gf_changelog_init(None) == -1:
        _raise_changelog_err()


def register(brick, path, log_file, log_level, retries=0):
    _init()

    ret = libgfc.gf_changelog_register(
        brick.encode(),
        path.encode(),
        log_file.encode(),
        log_level,
        retries)

    if ret == -1:
        _raise_changelog_err()


def scan():
    ret = libgfc.gf_changelog_scan()
    if ret == -1:
        _raise_changelog_err()


def startfresh():
    ret = libgfc.gf_changelog_start_fresh()
    if ret == -1:
        _raise_changelog_err()


def getchanges():
    def clsort(cfile):
        return cfile.split('.')[-1]

    changes = []
    buf = create_string_buffer(b'\0', 4096)
    call = libgfc.gf_changelog_next_change

    while True:
        ret = call(buf, 4096)
        if ret in (0, -1):
            break

        result = ''.join([chr(b) for b in buf.raw[:ret - 1]])
        changes.append(result)

    if ret == -1:
        _raise_changelog_err()

    # cleanup tracker
    startfresh()

    return sorted(changes, key=clsort)


def done(clfile):
    ret = libgfc.gf_changelog_done(clfile.encode())
    if ret == -1:
        _raise_changelog_err()


def history_scan():
    ret = libgfc.gf_history_changelog_scan()
    if ret == -1:
        _raise_changelog_err()

    return ret


def history_changelog(changelog_path, start, end, num_parallel):
    actual_end = c_ulong()
    ret = libgfc.gf_history_changelog(changelog_path.encode(), start, end,
                                      num_parallel, byref(actual_end))
    if ret == -1:
        _raise_changelog_err()

    if ret == -2:
        raise ChangelogHistoryNotAvailable()

    return (ret, actual_end.value)


def history_startfresh():
    ret = libgfc.gf_history_changelog_start_fresh()
    if ret == -1:
        _raise_changelog_err()


def history_getchanges():
    def clsort(cfile):
        return cfile.split('.')[-1]

    changes = []
    buf = create_string_buffer(b'\0', 4096)
    call = libgfc.gf_history_changelog_next_change

    while True:
        ret = call(buf, 4096)
        if ret in (0, -1):
            break

        result = ''.join([chr(b) for b in buf.raw[:ret - 1]])
        changes.append(result)

    if ret == -1:
        _raise_changelog_err()

    return sorted(changes, key=clsort)


def history_done(clfile):
    ret = libgfc.gf_history_changelog_done(clfile.encode())
    if ret == -1:
        _raise_changelog_err()


def delete_processed_changelogs(cfiles):
    """
    Backend changelogs will not be touched, only Changelog files
    from Working directory is deleted.
    """
    for cfile in cfiles:
        try:
            os.remove(cfile)
        except OSError as err:
            logging.warning(logf(
                "failed to remove processed changelog file",
                path=cfile,
                error=err
            ))
