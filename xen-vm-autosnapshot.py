#!/usr/bin/python
# -*- coding: utf-8 -*-
################################################################################
# XenServer VM automatic snapshot rotation script
# Copyright (c) 2009 Michael Conigliaro <mike [at] conigliaro [dot] org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
################################################################################
import getpass
import logging
import logging.handlers
import optparse
import re
import sys
import time

import XenAPI


APP_VERSION = "1.2"
APP_AUTHOR = "Michael T. Conigliaro <mike [at] conigliaro [dot] org>"
APP_WEBSITE = "http://conigliaro.org/"


def snapshot():
    """Take a snapshot of each VM."""

    log.debug("Starting snapshot routine")

    re_vmnames = re.compile(options.vm_regex)

    all_vms = session.xenapi.VM.get_all_records()

    # loop through vm record map
    for vm in all_vms:
        vm_record = all_vms[vm]

        # select appropriate vm
        if re_vmnames.match(vm_record["name_label"]) and \
           not vm_record["is_a_template"] and \
           not vm_record["is_control_domain"]:
            log.debug("Selecting VM: %s (%s)" %
                (vm_record["name_label"], vm_record["uuid"]))

            # snapshot name is based on time/tag
            snapshot_name = "%s: %s %s" % (vm_record["name_label"],
                                        time.strftime("%Y-%m-%d %H:%M:%S"),
                                        options.snapshot_tag)

            # create snapshot
            log.info("Creating VM snapshot: %s" % snapshot_name)
            if not options.dry_run:
                done = False
                tries = 0
                while not done and tries <= options.retry_max:
                    if tries:
                        log.info("Retrying in %d seconds [%d/%d]" %
                            (options.retry_delay, tries, options.retry_max))
                        time.sleep(options.retry_delay)
                    try:
                        tries += 1
                        if options.snapshot_with_quiesce:
                            session.xenapi.VM.snapshot_with_quiesce(vm, snapshot_name)
                        else:
                            session.xenapi.VM.snapshot(vm, snapshot_name)
                        done = True
                    except Exception, e:
                        log.error("Unhandled exception: %s" % str(e))
                        #raise


def snapshot_rotate():
    """Rotate old snapshots for each VM. When destroying old VM snapshots, all
    corresponding VDI snapshots will be destroyed as well."""

    log.debug("Starting snapshot rotation routine")

    re_vmnames = re.compile(options.vm_regex)

    all_vms = session.xenapi.VM.get_all_records()
    all_vbds = session.xenapi.VBD.get_all_records()
    all_vdis = session.xenapi.VDI.get_all_records()

    # loop through vm record map
    for vm in all_vms:
        vm_record = all_vms[vm]

        # select appropriate vm
        if re_vmnames.match(vm_record["name_label"]) and \
           not vm_record["is_a_template"] and \
           not vm_record["is_control_domain"]:
            log.debug("Selecting VM: %s (%s)" % (vm_record["name_label"],
                                                 vm_record["uuid"]))

            # create list of snapshots (with matching tag only)
            snapshot_count = 0
            vm_snapshots = {}
            for snapshot in vm_record["snapshots"]:
                if all_vms[snapshot]["name_label"].endswith(' ' + options.snapshot_tag):
                    vm_snapshots[snapshot] = all_vms[snapshot]

            # check snapshot count
            snapshot_count = len(vm_snapshots)
            log.debug("Found %d snapshot(s)" % snapshot_count)
            if snapshot_count > options.snapshot_max:

                # sort snapshots by date, oldest first
                vm_snapshots = sorted(vm_snapshots,
                               key=lambda x: all_vms[x]["snapshot_time"])

                # loop through old snapshots
                for snapshot in vm_snapshots[0:snapshot_count - options.snapshot_max]:

                    # destroy old vm snapshot
                    snapshot_record = all_vms[snapshot]
                    log.debug("Selecting VM snapshot: %s (%s)" %
                        (snapshot_record["name_label"], snapshot_record["uuid"]))
                    log.info("Destroying VM snapshot: %s" % snapshot_record["name_label"])
                    if not options.dry_run:
                        done = False
                        tries = 0
                        while not done and tries <= options.retry_max:
                            if tries:
                                log.info("Retrying in %d seconds [%d/%d]" %
                                    (options.retry_delay, tries, options.retry_max))
                                time.sleep(options.retry_delay)
                            try:
                                tries += 1
                                session.xenapi.VM.destroy(snapshot)
                                done = True
                            except Exception, e:
                                log.error("Unhandled exception: %s" % str(e))
                                #raise

                    # loop through this snapshot's vbds (disks only)
                    for vbd in snapshot_record["VBDs"]:
                        vbd_record = all_vbds[vbd]
                        if vbd_record["type"] == "Disk":

                            # destroy corresponding vdi
                            vdi_record = all_vdis[vbd_record["VDI"]]
                            vdi = session.xenapi.VDI.get_by_uuid(vdi_record["uuid"])
                            log.debug("Selecting VDI snapshot: %s (%s)" %
                                (vdi_record["name_label"], vdi_record["uuid"]))
                            log.info("Destroying VDI snapshot: %s" % vdi_record["name_label"])
                            if not options.dry_run:
                                done = False
                                tries = 0
                                while not done and tries <= options.retry_max:
                                    if tries:
                                        log.info("Retrying in %d seconds [%d/%d]" %
                                            (options.retry_delay, tries, options.retry_max))
                                        time.sleep(options.retry_delay)
                                    try:
                                        tries += 1
                                        session.xenapi.VDI.destroy(vdi)
                                        done = True
                                    except Exception, e:
                                        log.error("Unhandled exception: %s" % str(e))
                                        #raise


if __name__ == "__main__":

    # define command line options
    valid_args = ['snapshot', 'snapshot-rotate']
    op = optparse.OptionParser("usage: %prog [options] <" +
        ' '.join(map(lambda x: "[%s]" % x, valid_args)) + ">",
        version="%%prog v%s\nAuthor: %s\nWebsite: %s" % (APP_VERSION, APP_AUTHOR, APP_WEBSITE))

    og_sess = optparse.OptionGroup(op, "Session Options")
    og_sess.add_option('--server',
                       dest='server',
                       help="xenserver host (default: %default)")
    og_sess.add_option('--username',
                       dest='username',
                       help="xenserver username (default: %default)")
    og_sess.add_option('--password',
                       dest='password',
                       help="xenserver password")
    og_sess.add_option('--dry-run',
                       dest='dry_run',
                       action='store_true',
                       help="perform a trial run with no changes")
    op.add_option_group(og_sess)

    og_vm = optparse.OptionGroup(op, "VM Selection Options")
    og_vm.add_option('--vms',
                     dest='vm_regex',
                     help="regular expression for selecting VMs (default: %default)")
    op.add_option_group(og_vm)

    og_snap = optparse.OptionGroup(op, "Snapshot Options")
    og_snap.add_option('--quiesce',
                       dest="snapshot_with_quiesce",
                       action='store_true',
                       help="snapshot with quiesce")
    og_snap.add_option('--snapshot-max',
                       dest='snapshot_max',
                       type="int",
                       help="number of snapshots to keep when rotating (default: %default)")
    og_snap.add_option('--snapshot-tag',
                       dest="snapshot_tag",
                       help="snapshot tag (default: %default)")
    op.add_option_group(og_snap)

    og_re = optparse.OptionGroup(op, "Retry Options")
    og_re.add_option('--retry-max',
                     dest='retry_max',
                     type="int",
                     help="number of times to retry failed operations (default: %default)")
    og_re.add_option('--retry-delay',
                     dest='retry_delay',
                     type="int",
                     help="seconds of delay between retries (default: %default)")
    op.add_option_group(og_re)

    og_log = optparse.OptionGroup(op, "Output and Logging Options")
    og_log.add_option('--log-level',
                      dest='log_level',
                      help="critical, error, warning, info, debug (default: %default)")
    og_log.add_option('--log-file-path',
                      dest='log_file_path',
                      help="path for optional log file")
    og_log.add_option('--log-file-rotate-interval-type',
                      dest='log_file_rotate_interval_type',
                      help="s=seconds, m=minutes h=hours, d=days, w=week day (0=monday), midnight (default: %default)")
    og_log.add_option('--log-file-rotate-interval',
                      dest='log_file_rotate_interval',
                      type="int",
                      help="log rotation interval (default: %default)")
    og_log.add_option('--log-file-max-backups',
                      dest='log_file_max_backups',
                      type="int",
                      help="number of log files to keep when rotating (default: %default)")
    op.add_option_group(og_log)

    op.set_defaults(server = 'localhost',
                    username = getpass.getuser(),
                    password = '',
                    retry_max = 2,
                    retry_delay = 10,
                    vm_regex = '^$',
                    snapshot_max = 1,
                    snapshot_tag = '(auto)',
                    log_level = 'info',
                    log_file_rotate_interval_type = 'd',
                    log_file_rotate_interval = 7,
                    log_file_max_backups = 4)

    # parse and validate command line arguments
    (options, args) = op.parse_args()
    if (not len(args)):
        op.error("You must supply an argument")
    for arg in args:
        if arg not in valid_args:
            op.error("Invalid argument: " + arg)

    # set up logging
    log = logging.getLogger()
    options.log_level = options.log_level.upper()
    if options.log_level == 'CRITICAL':
        log.setLevel(logging.CRITICAL)
    elif options.log_level == 'ERROR':
        log.setLevel(logging.ERROR)
    elif options.log_level == 'WARNING':
        log.setLevel(logging.WARNING)
    elif options.log_level == 'INFO':
        log.setLevel(logging.INFO)
    elif options.log_level == 'DEBUG':
        log.setLevel(logging.DEBUG)
    consoleLogger = logging.StreamHandler()
    consoleLogger.setFormatter(
            logging.Formatter("%(levelname)s - %(message)s"))
    log.addHandler(consoleLogger)
    if options.log_file_path:
        fileLogger = logging.handlers.TimedRotatingFileHandler(
            filename = options.log_file_path,
            when = options.log_file_rotate_interval_type,
            interval = options.log_file_rotate_interval,
            backupCount = options.log_file_max_backups)
        fileLogger.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        log.addHandler(fileLogger)

    log.debug("Running with: options=%s args=%s" % (options, args))

    try:
        # log in
        log.debug("Starting XenServer session")
        session = XenAPI.Session('https://' + options.server)
        session.xenapi.login_with_password(options.username, options.password)

    except Exception, e:
        log.critical("Unable to start XenAPI session: %s" % str(e))
        sys.exit(1)

    try:
        # map arguments to functions
        for arg in args:
            if (arg == 'snapshot'):
                snapshot()
            elif (arg == 'snapshot-rotate'):
                snapshot_rotate()

    except Exception, e:
         log.critical("Unhandled exception: %s" % str(e))
         raise

    finally:
        # log out
        log.debug("Ending XenServer session")
        session.xenapi.session.logout()