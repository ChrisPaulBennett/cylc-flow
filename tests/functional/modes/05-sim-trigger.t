#!/usr/bin/env bash
# THIS FILE IS PART OF THE CYLC WORKFLOW ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
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

# Test that we can re-trigger a task in sim mode

. "$(dirname "$0")/test_header"
set_test_number 4

install_workflow "${TEST_NAME_BASE}" "${TEST_NAME_BASE}"
run_ok "${TEST_NAME_BASE}-validate" cylc validate "${WORKFLOW_NAME}"
workflow_run_ok "${TEST_NAME_BASE}-start" \
    cylc play "${WORKFLOW_NAME}" --mode=simulation
SCHD_LOG="${WORKFLOW_RUN_DIR}/log/scheduler/log"

# Wait for the workflow to stall, then check for first task failure:
poll_grep_workflow_log 'stall timer starts'
grep_ok \
    '\[1/fail_fail_fail running job:01 flows:1\] => failed' \
    "${SCHD_LOG}"

# Trigger task again, wait for it to send a warning and check that it
# too has failed:
cylc trigger "${WORKFLOW_NAME}//1/fail_fail_fail"
poll_grep_workflow_log \
    'job:02.*did not complete'
grep_ok \
    '\[1/fail_fail_fail running job:02 flows:1\] => failed' \
    "${SCHD_LOG}"

purge
