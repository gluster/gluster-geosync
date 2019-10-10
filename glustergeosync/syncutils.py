import os
from queue import Queue, Empty
import time
from threading import Thread


class SyncBatch:
    def __init__(self, sync_func, num_jobs=3, batch_size=1000):
        self.num_jobs = num_jobs
        self.batch_size = batch_size
        self.sync_func = sync_func
        self.queue = Queue()
        self.syncjobs = []

        # Start as many workers required
        for i in range(num_jobs):
            job = Thread(target=self._worker, args=(i+1, ))
            job.start()
            self.syncjobs.append(job)

    def _get_batch(self):
        items = []
        num_items = 0
        while True:
            # If Batch is ready with the number of items it can handle
            if num_items >= self.batch_size:
                break

            try:
                item = self.queue.get(block=False)
                self.queue.task_done()

                # None is exit condition, send remaining items and
                # call it DONE, so that worker will also exit after
                # processing this last list of items
                if item is None:
                    return (items, True)

                # Keep collecting the items
                items.append(item)
                num_items += 1
            except Empty:
                # May be delay, it is not end of incoming jobs
                break

        return (items, False)

    def _worker(self, label):
        while True:
            to_sync, done = self._get_batch()
            if to_sync:
                self.sync_func(label, to_sync)

            # Nothing else to process
            if done:
                break

            time.sleep(0.5)

    def enqueue(self, items):
        for item in items:
            self.queue.put(item)

    def wait(self):
        for _ in self.syncjobs:
            self.queue.put(None)

        self.queue.join()
        for syncjob in self.syncjobs:
            syncjob.join()


def logf(message, **kwargs):
    """
    Log Format helper function, log messages can be
    easily modified to structured log format.
    lf("Config Change", sync_jobs=4, brick=/bricks/b1) will be
    converted as "Config Change [{brick=/bricks/b1}, {sync_jobs=4}]"
    """
    msgparts = []
    for field, value in kwargs.items():
        msgparts.append("{%s=%s}" % (field, value))

    if msgparts:
        message += (" [" + ", ".join(msgparts) + "]")

    return message


ROOT_GFID = "00000000-0000-0000-0000-000000000001"


def symlink_gfid_to_path(brick, gfid):
    """
    Each directories are symlinked to file named GFID
    in .glusterfs directory of brick backend. Using readlink
    we get PARGFID/basename of dir. readlink recursively till
    we get PARGFID as ROOT_GFID.
    """
    if gfid == ROOT_GFID:
        return ""

    out_path = ""
    while True:
        path = os.path.join(brick, ".glusterfs", gfid[0:2], gfid[2:4], gfid)
        path_readlink = os.readlink(path)
        pgfid = os.path.dirname(path_readlink)
        out_path = os.path.join(os.path.basename(path_readlink), out_path)
        if pgfid == "../../00/00/%s" % ROOT_GFID:
            break
        gfid = os.path.basename(pgfid)
    return out_path


def gfid_to_paths(brick, gfid):
    import xattr

    gfidfile = os.path.join(brick, ".glusterfs", gfid[0:2], gfid[2:4], gfid)
    paths = []
    gfids = []
    try:
        for xtr in xattr.list(gfidfile):
            if xtr.startswith(b"trusted.gfid2path."):
                data = xattr.get(gfidfile, xtr).decode().split("/")
                pgfid = data[-2]
                basename = data[-1]
                ppath = symlink_gfid_to_path(brick, pgfid)
                paths.append(os.path.join(ppath, basename))
    except (OSError, IOError):
        gfids.append(gfid)

    return (paths, gfids)


GLUSTER_LOG_LEVELS = [
    "NONE",
    "EMERG",
    "ALERT",
    "CRITICAL",
    "ERROR",
    "WARNING",
    "NOTICE",
    "INFO",
    "DEBUG",
    "TRACE"
]


def get_changelog_log_level(lvl):
    return GLUSTER_LOG_LEVELS.index(lvl)


def get_brick_stime(brick):
    return 0


def update_brick_stime(brick, last_synced):
    pass
