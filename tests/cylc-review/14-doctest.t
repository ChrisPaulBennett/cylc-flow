#!/bin/bash
# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2018 NIWA & British Crown (Met Office) & Contributors.
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
#-------------------------------------------------------------------------------
# Run doctests in the cylc review codebase.
#-------------------------------------------------------------------------------
. "$(dirname "$0")/test_header"
set_test_number 2
#-------------------------------------------------------------------------------
TEST_NAME="${TEST_NAME_BASE}"
run_ok "${TEST_NAME}" python -m doctest "${CYLC_DIR}/lib/cylc/review.py"
sed -i /1034h/d "${TEST_NAME}.stdout"  # Remove some nasty unicode output.
cmp_ok "${TEST_NAME}.stdout" "${TEST_NAME}.stdout" /dev/null