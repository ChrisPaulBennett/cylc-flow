#!/bin/bash
#C: THIS FILE IS PART OF THE CYLC SUITE ENGINE.
#C: Copyright (C) 2008-2014 NIWA
#C:
#C: This program is free software: you can redistribute it and/or modify
#C: it under the terms of the GNU General Public License as published by
#C: the Free Software Foundation, either version 3 of the License, or
#C: (at your option) any later version.
#C:
#C: This program is distributed in the hope that it will be useful,
#C: but WITHOUT ANY WARRANTY; without even the implied warranty of
#C: MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#C: GNU General Public License for more details.
#C:
#C: You should have received a copy of the GNU General Public License
#C: along with this program.  If not, see <http://www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------
# Test validation with a  prev-style cycle synax and post-style retry syntax
. $(dirname $0)/test_header
#-------------------------------------------------------------------------------
set_test_number 2
#-------------------------------------------------------------------------------
install_suite $TEST_NAME_BASE $TEST_NAME_BASE
#-------------------------------------------------------------------------------
TEST_NAME=$TEST_NAME_BASE
run_fail $TEST_NAME cylc validate --debug -v -v $SUITE_NAME
grep_ok "Conflicting syntax: pre-cylc-6 syntax \
(integer interval: \[cylc\]\[event hooks\]timeout = 4320) \
vs post-cylc-6 syntax \
(ISO 8601 interval: \[runtime\]\[A\]retry delays = PT30M)" \
    $TEST_NAME.stderr
#-------------------------------------------------------------------------------
exit
