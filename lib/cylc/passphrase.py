#!/usr/bin/env python

# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2015 NIWA
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import re
import sys
import random
import string
from stat import *

import cylc.flags
from cylc.mkdir_p import mkdir_p
from cylc.suite_host import get_hostname, is_remote_host
from cylc.owner import user, is_remote_user

# TODO - Pyro passphrase handling could do with a complete overhaul, but
# it will soon be made obsolete by the upcoming communications refactor.


class PassphraseError(Exception):
    """
    Attributes:
        message - what the problem is.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class passphrase(object):
    """Pyro passphrase file utility."""

    def __init__(self, suite, owner=None, host=None):
        self.suite = suite
        self.owner = owner
        self.host = host
        if self.owner is None:
            self.owner = user
        if self.host is None:
            self.host = get_hostname()
        self.location = None

    def get_passphrase_file(self, pfile=None, suitedir=None):
        """
Passphrase location, order of preference:

1/ The pfile argument - used for passphrase creation by "cylc register".

2/ The suite definition directory, because suites may be automatically
installed (e.g. by Rose) to remote task hosts, and remote tasks know
this location from their execution environment. Local user command
invocations can use the suite registration database to find the suite
definition directory.  HOWEVER, remote user command invocations cannot
do this even if the local and remote hosts share a common filesystem,
because we cannot be sure if finding the expected suite registration
implies a common filesystem or a different remote suite that happens to
be registered under the same name. User accounts used for remote control
must therefore install the passphrase in the secondary standard
locations (below) or use the command line option to explicitly reveal
the location. Remote tasks with 'ssh messaging = True' look first in the
suite definition directory of the suite host, which they know through
the variable CYLC_SUITE_DEF_PATH_ON_SUITE_HOST in the task execution
environment.

3/ Secondary locations:
    (i) $HOME/.cylc/SUITE_HOST/SUITE_OWNER/SUITE_NAME/passphrase
   (ii) $HOME/.cylc/SUITE_HOST/SUITE_NAME/passphrase
  (iii) $HOME/.cylc/SUITE_NAME/passphrase
These are more sensible locations for remote suite control from accounts
that do not actually need the suite definition directory to be installed.
"""
        # 1/ Explicit suite definition directory given on the command line.
        if pfile:
            if os.path.isdir(pfile):
                pfile = os.path.join(pfile, 'passphrase')
            if os.path.isfile(pfile):
                self.set_location(pfile)
            else:
                # If an explicit location is given, the file must exist.
                raise PassphraseError(
                    'ERROR, file not found on %s@%s: %s' % (
                        user, get_hostname(), pfile))

        # 2/ Cylc commands with suite definition directory from local reg.
        if not self.location and suitedir:
            pfile = os.path.join(suitedir, 'passphrase')
            if os.path.isfile(pfile):
                self.set_location(pfile)

        # (2 before 3 else sub-suites load their parent suite's
        # passphrase on start-up because the "cylc run" command runs in
        # a parent suite task execution environment).

        # 3/ Running tasks: suite def dir from the task execution environment.
        if not self.location:
            try:
                # Test for presence of task execution environment
                suite_host = os.environ['CYLC_SUITE_HOST']
                suite_owner = os.environ['CYLC_SUITE_OWNER']
            except KeyError:
                # not called by a task
                pass
            else:
                # called by a task
                if is_remote_host(suite_host) or is_remote_user(suite_owner):
                    # 2(i)/ Task messaging call on a remote account.
                    # First look in the remote suite run directory than suite
                    # definition directory ($CYLC_SUITE_DEF_PATH is modified
                    # for remote tasks):
                    for key in ['CYLC_SUITE_RUN_DIR', 'CYLC_SUITE_DEF_PATH']:
                        if key in os.environ:
                            pfile = os.path.join(os.environ[key], 'passphrase')
                            if os.path.isfile(pfile):
                                self.set_location(pfile)
                                break
                else:
                    # 2(ii)/ Task messaging call on the suite host account.

                    # Could be a local task or a remote task with 'ssh
                    # messaging = True'. In either case use
                    # $CYLC_SUITE_DEF_PATH_ON_SUITE_HOST which never
                    # changes, not $CYLC_SUITE_DEF_PATH which gets
                    # modified for remote tasks as described above.
                    try:
                        pfile = os.path.join(
                            os.environ['CYLC_SUITE_DEF_PATH_ON_SUITE_HOST'],
                            'passphrase')
                    except KeyError:
                        pass
                    else:
                        if os.path.isfile(pfile):
                            self.set_location(pfile)

        # 4/ Other allowed locations, as documented above.
        if not self.location:
            locations = []
            # For remote control commands, self.host here will be fully
            # qualified or not depending on what's given on the command line.
            short_host = re.sub('\..*', '', self.host)
            prefix = os.path.join(os.environ['HOME'], '.cylc')
            locations.append(
                os.path.join(
                    prefix, self.host, self.owner, self.suite, 'passphrase'))
            if short_host != self.host:
                locations.append(os.path.join(
                    prefix, short_host, self.owner, self.suite, 'passphrase'))
            locations.append(
                os.path.join(prefix, self.host, self.suite, 'passphrase'))
            if short_host != self.host:
                locations.append(os.path.join(
                    prefix, short_host, self.suite, 'passphrase'))
            locations.append(os.path.join(prefix, self.suite, 'passphrase'))
            for pfile in locations:
                if os.path.isfile(pfile):
                    self.set_location(pfile)
                    break

        if not self.location:
            raise PassphraseError(
                'ERROR: passphrase for suite %s not found on %s@%s' % (
                    self.suite, user, get_hostname()))
        return self.location

    def set_location(self, pfile):
        if cylc.flags.debug:
            print '%s (%s@%s)' % (pfile, user, get_hostname())
        self.location = pfile

    def generate(self, dir):
        pfile = os.path.join(dir, 'passphrase')
        if os.path.isfile(pfile):
            try:
                self.get(pfile)
                return
            except PassphraseError:
                pass
        # Note: Perhaps a UUID might be better here?
        char_set = (
            string.ascii_uppercase + string.ascii_lowercase + string.digits)
        self.passphrase = ''.join(random.sample(char_set, 20))
        mkdir_p(dir)
        f = open(pfile, 'w')
        f.write(self.passphrase)
        f.close()
        # set passphrase file permissions to owner-only
        os.chmod(pfile, 0600)
        if cylc.flags.verbose:
            print 'Generated suite passphrase: %s@%s:%s' % (
                user, get_hostname(), pfile)

    def get(self, pfile=None, suitedir=None):
        ppfile = self.get_passphrase_file(pfile, suitedir)
        psf = open(ppfile, 'r')
        lines = psf.readlines()
        psf.close()
        if len(lines) != 1:
            raise PassphraseError(
                'ERROR, invalid passphrase file: %s@%s:%s' % (
                    user, get_hostname(), ppfile))
        # chomp trailing whitespace and newline
        self.passphrase = lines[0].strip()
        return self.passphrase


def get_passphrase(suite, owner, host, db):
    """Find a suite passphrase."""
    try:
        # Local suite, retrieve suite definition directory location.
        suitedir = os.path.dirname(db.get_suiterc(suite))
    except db.Error:
        suitedir = None
    return passphrase(suite, owner, host).get(None, suitedir)
