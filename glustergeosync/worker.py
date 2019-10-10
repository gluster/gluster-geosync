import time
import logging
import subprocess
from argparse import ArgumentParser

from glustergeosync import changelogapi
from glustergeosync.syncutils import SyncBatch, logf, gfid_to_paths, \
    get_brick_stime, update_brick_stime, get_changelog_log_level

IDX_START = 0
IDX_END = 2
UNLINK_ENTRY = 2

POS_GFID = 0
POS_TYPE = 1
POS_ENTRY1 = -1

TYPE_META = "M "
TYPE_GFID = "D "
TYPE_ENTRY = "E "
CHANGELOG_CONN_RETRIES = 5


def syncjob(label, tasks, fromdir, destdir):
    # TODO: Use proper args for ssh and other tunings for rsync
    cmd = ["rsync", "-0", "--files-from=-", fromdir, destdir]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    proc.stdin.write("\x00".join(tasks))
    out, err = proc.communicate()
    print(out, err)

    print(logf(
        "Sync Job completed",
        label=label,
        tasks=tasks
    ))


def paths_from_changelog(brick, cfilepath):
    gfids = set()
    paths = []
    failed = []
    with open(cfilepath, "r") as cfile:
        clist = cfile.readlines()

    for line in clist:
        line = line.strip()
        op_type = line[IDX_START:IDX_END]   # entry type
        entry = line[IDX_END:].split(' ')      # rest of the bits

        if op_type == TYPE_ENTRY:
            entry_type = entry[POS_TYPE]
            if entry_type in ['UNLINK', 'RMDIR']:
                # TODO: Get Old path and handle ignore deletes
                pass
            elif entry_type in ['CREATE', 'MKDIR', 'MKNOD', 'LINK', 'SYMLINK', 'RENAME']:
                gfids.add(entry[POS_GFID])
                if entry_type == "RENAME":
                    # TODO: Get path for old name and add to paths
                    pass
        elif op_type in [TYPE_GFID, TYPE_META]:
            gfids.add(entry[POS_GFID])
        else:
            logging.warning(logf('got invalid fop type', type=op_type))

    for gfid in gfids:
        converted_paths, failed_gfids = gfid_to_paths(brick, gfid)
        paths += converted_paths
        failed += failed_gfids

    # TODO: Try to find GFID to Path from Mount point for the failed gfids
    if failed:
        logging.warning(logf(
            "failed to convert GFID to path. Rebalance may have "
            "moved the file or GFID to path xattrs are not updated",
            gfids=" ".join(set(failed))
        ))

    return set(paths)


def process_changelogs(brick):
    start_time = int(time.time())
    scan_time = 0
    cleanup_time = 0
    sync_time = 0

    # Scan and get new changes
    changelogapi.scan()
    changes = changelogapi.getchanges()

    # Collect scan time
    scan_time = int(time.time()) - start_time

    if not changes:
        logging.debug("nothing to process. no changelogs")
        return

    # Identify already processed changelogs and
    # delete the changelog file which is already processed.
    start_idx = 0
    stime = get_brick_stime(brick)
    for idx, change in enumerate(changes):
        change_ts = int(change.split('.')[-1])
        if change_ts < stime:
            start_idx = idx
        else:
            break

    changelogapi.delete_processed_changelogs(changes[0:start_idx])

    # Collect cleanup time
    cleanup_time = int(time.time()) - start_time - scan_time

    if not changes[start_idx:]:
        logging.debug(logf(
            "nothing to process after skipping processed changelogs",
            stime=stime
        ))
        return

    def syncjob_wrapper(label, tasks):
        syncjob(label, tasks, "/mnt/gvol4", "/mnt/bkp_gvol4")

    syncbatch = SyncBatch(syncjob_wrapper)
    for change in changes[start_idx:]:
        to_sync = paths_from_changelog(brick, change)
        print(to_sync)
        syncbatch.enqueue(to_sync)

    # Wait till all sync jobs are complete in a batch
    syncbatch.wait()

    # Collect Sync time
    sync_time = int(time.time()) - start_time - cleanup_time

    # Last value in the above for loop
    change_ts = int(change.split('.')[-1])
    update_brick_stime(brick, change_ts)
    changelogapi.delete_processed_changelogs(changes[start_idx:])

    logging.debug(logf(
        "Batch completed",
        changelog_start=changes[0].split(".")[-1],
        changelog_end=changes[-1].split(".")[-1],
        scan_time=scan_time,
        cleanup_time=cleanup_time,
        sync_time=sync_time,
        total_time=(int(time.time()) - start_time)
    ))


def start(input_args):
    parser = ArgumentParser()
    parser.add_argument("--brick", help="Brick Path", required=True)
    parser.add_argument("--workdir", help="Workdir for Changelog processing", required=True)
    parser.add_argument("--changelog-logfile", help="Changelog Log file path", required=True)
    parser.add_argument("--changelog-loglevel", help="Changelog Log level",
                        default="INFO")

    args = parser.parse_args(input_args)

    # Register to Live changelogs
    changelogapi.register(
        args.brick,
        args.workdir,
        args.changelog_logfile,
        get_changelog_log_level(args.changelog_loglevel),
        retries=CHANGELOG_CONN_RETRIES
    )
    crawls = 0
    while True:
        process_changelogs(args.brick)

        crawls += 1
        time.sleep(2)
