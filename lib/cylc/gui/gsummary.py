#!/usr/bin/env python

#C: THIS FILE IS PART OF THE CYLC SUITE ENGINE.
#C: Copyright (C) 2008-2013 Hilary Oliver, NIWA
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

import copy
import datetime
import os
import re
import shlex
import subprocess
import threading
import time

import gtk
import gobject
#import pygtk
#pygtk.require('2.0')

from cylc.gui.gcylc_config import config
from cylc.gui.SuiteControl import run_get_stdout
from cylc.gui.DotMaker import DotMaker
from cylc.gui.util import get_icon, setup_icons
from cylc.owner import user

PYRO_TIMEOUT = 2


def get_host_suites(hosts, timeout=None, owner=None):
    """Return a dictionary of hosts and their running suites."""
    if owner is None:
        owner = user
    host_suites_map = {}
    if timeout is None:
        timeout = PYRO_TIMEOUT
    for host in hosts:
        host_suites_map[host] = []
        command = ("cylc scan " +
                   "--host={0} --owner={1} --pyro-timeout={2}".format(
                                         host, owner, timeout))
        res, stdout = run_get_stdout(command)
        if res and stdout:
            for line in stdout:
                if line:
                    host_suites_map[host].append(line.split()[0])
    return host_suites_map


def get_status_tasks(host, suite, owner=None):
    if owner is None:
        owner = user
    command = "cylc cat-state --host={0} --owner={1} {2}".format(
                                                 host, owner, suite)
    res, stdout = run_get_stdout(command)
    stdout = "\n".join(stdout)
    if not res:
        return None
    status_tasks = {}
    for line in stdout.rpartition("Begin task states")[2].splitlines():
        task_result = re.match("([^ ]+) : status=([^,]+), spawned", line)
        if not task_result:
            continue
        task, status = task_result.groups()
        status_tasks.setdefault(status, [])
        status_tasks[status].append(task)
    return status_tasks


def launch_gcylc(host, suite, owner=None):
    if owner is None:
        owner = user
    stdout = open(os.devnull, "w")
    stderr = stdout
    command = "cylc gui --host={0} --owner={1} {2}".format(
                                         host, owner, suite)
    command = shlex.split(command)
    subprocess.Popen(command, stdout=stdout, stderr=stderr)


class SummaryApp(object):

    """Summarize running suite statuses for a given set of hosts."""

    def __init__(self, hosts, owner=None):
        gobject.threads_init()
        setup_icons()
        self.hosts = hosts
        if owner is None:
            owner = user
        self.owner = owner
        self.window = gtk.Window()
        self.window.set_title("cylc gsummary")
        self.window.set_icon(get_icon())
        self.vbox = gtk.VBox()
        self.vbox.show()
        self.usercfg = config().cfg
        self.theme_name = self.usercfg['use theme'] 
        self.theme = self.usercfg['themes'][self.theme_name]
        self.dots = DotMaker(self.theme)
        suite_treemodel = gtk.TreeStore(*([str, str, bool, int] + [str] * 20))
        self.suite_treeview = gtk.TreeView(suite_treemodel)
        
        # Construct the host column.
        host_name_column = gtk.TreeViewColumn("Host")
        cell_text_host = gtk.CellRendererText()
        host_name_column.pack_start(cell_text_host, expand=False)
        host_name_column.set_attributes(cell_text_host, text=0)
        host_name_column.set_sort_column_id(0)
        host_name_column.set_visible(False)
        
        # Construct the suite column.
        suite_name_column = gtk.TreeViewColumn("Suite")
        cell_text_name = gtk.CellRendererText()
        suite_name_column.pack_start(cell_text_name, expand=False)
        suite_name_column.set_cell_data_func(
                   cell_text_name, self._set_cell_text_name)
        suite_name_column.set_sort_column_id(1)

        # Construct the update time column.
        time_column = gtk.TreeViewColumn("Updated")
        cell_text_time = gtk.CellRendererText()
        time_column.pack_start(cell_text_time, expand=False)
        time_column.set_cell_data_func(
                    cell_text_time, self._set_cell_text_time)
        time_column.set_sort_column_id(2)
        time_column.set_visible(False)

        # Construct the status column.
        status_column = gtk.TreeViewColumn("Status")
        status_column.set_sort_column_id(4)
        for i in range(4, 24):
            cell_pixbuf_state = gtk.CellRendererPixbuf()
            status_column.pack_start(cell_pixbuf_state, expand=False)
            status_column.set_cell_data_func(
                   cell_pixbuf_state, self._set_cell_pixbuf_state, i)
        
        self.suite_treeview.append_column(host_name_column)
        self.suite_treeview.append_column(suite_name_column)
        self.suite_treeview.append_column(time_column)
        self.suite_treeview.append_column(status_column)
        self.suite_treeview.grab_focus()
        self.suite_treeview.show()
        self.suite_treeview.connect("button-press-event",
                                    self._on_button_press_event)
        scrolled_window = gtk.ScrolledWindow()
        scrolled_window.set_policy(gtk.POLICY_AUTOMATIC,
                                   gtk.POLICY_AUTOMATIC)
        scrolled_window.add(self.suite_treeview)
        scrolled_window.show()
        self.vbox.pack_start(scrolled_window, expand=True, fill=True)
        self.updater = SummaryAppUpdater(self.hosts, suite_treemodel,
                                         owner=self.owner)
        self.updater.start()
        self.window.add(self.vbox)
        self.window.connect("destroy", self._on_destroy_event)
        self.window.set_default_size(200, 100)
        self.window.show()

    def _on_button_press_event(self, treeview, event):
        # DISPLAY MENU ONLY ON RIGHT CLICK ONLY
        if event.button != 3:
            return False
        menu = gtk.Menu()

        treemodel = treeview.get_model()

        x = int(event.x)
        y = int(event.y)
        time = event.time
        pth = treeview.get_path_at_pos(x, y)
        
        if pth is not None:
            # Add a gcylc launcher item.
            path, col, cellx, celly = pth
            
            iter_ = treemodel.get_iter(path)
            host, suite = treemodel.get(iter_, 0, 1)
            gcylc_item = gtk.ImageMenuItem(stock_id="gcylc")
            gcylc_item.set_label("Launch gcylc")
            gcylc_item.connect("button-press-event",
                               lambda b, e: launch_gcylc(
                                                   host, suite, self.owner))
            gcylc_item.show()
            menu.append(gcylc_item)
            sep_item = gtk.SeparatorMenuItem()
            sep_item.show()
            menu.append(sep_item)
        
        # Construct column view chooser items.
        view_item = gtk.ImageMenuItem(stock_id=gtk.STOCK_INDEX)
        view_item.set_label("View Column...")
        view_item.show()
        view_menu = gtk.Menu()
        view_item.set_submenu(view_menu)
        for column_index, column in enumerate(treeview.get_columns()):
            name = column.get_title()
            is_visible = column.get_visible()
            column_item = gtk.CheckMenuItem(name.replace("_", "__"))
            column_item._connect_args = (column_index, is_visible)
            column_item.set_active(is_visible)
            column_item.connect("toggled", self._on_toggle_column_visible)
            column_item.show()
            view_menu.append(column_item)
        menu.append(view_item)
        sep_item = gtk.SeparatorMenuItem()
        sep_item.show()
        menu.append(sep_item)
        
        # Construct theme chooser items (same as cylc.gui.SuiteControl).
        theme_item = gtk.ImageMenuItem('Theme')
        img = gtk.image_new_from_stock(gtk.STOCK_SELECT_COLOR, gtk.ICON_SIZE_MENU)
        theme_item.set_image(img)
        thememenu = gtk.Menu()
        theme_item.set_submenu(thememenu)
        theme_item.show()

        theme_items = {}
        theme = "default"
        theme_items[theme] = gtk.RadioMenuItem(label=theme)
        thememenu.append(theme_items[theme])
        self._set_tooltip(theme_items[theme], theme + " task state theme")
        theme_items[theme].theme_name = theme
        for theme in self.usercfg['themes']:
            if theme == "default":
                continue
            theme_items[theme] = gtk.RadioMenuItem(group=theme_items['default'], label=theme)
            thememenu.append(theme_items[theme])
            self._set_tooltip(theme_items[theme], theme + " task state theme")
            theme_items[theme].theme_name = theme

        # set_active then connect, to avoid causing an unnecessary toggle now.
        theme_items[self.theme_name].set_active(True)
        for theme in self.usercfg['themes']:
            theme_items[theme].show()
            theme_items[theme].connect('toggled', self._set_theme)       

        menu.append(theme_item)
        sep_item = gtk.SeparatorMenuItem()
        sep_item.show()
        menu.append(sep_item)
        
        # Construct a clean stopped suites item.
        row_iter = treemodel.get_iter_first()
        has_stopped_suites = False
        while row_iter is not None:
            if treemodel.get_value(row_iter, 2):
                has_stopped_suites = True
                break
            row_iter = treemodel.iter_next(row_iter)
        clear_item = gtk.ImageMenuItem(stock_id=gtk.STOCK_CLEAR)
        clear_item.set_label("Clear Stopped Suites")
        clear_item.show()
        clear_item.set_sensitive(has_stopped_suites)
        clear_item.connect("button-press-event",
                           lambda b, e: self.updater.clear_stopped_suites())
        menu.append(clear_item)
        menu.popup( None, None, None, event.button, event.time )
        return False

    def _on_destroy_event(self, widget):
        self.updater.quit = True
        gtk.main_quit()
        return False

    def _on_toggle_column_visible(self, menu_item):
        column_index, is_visible = menu_item._connect_args
        column = self.treeview.get_columns()[index]
        column.set_visible(not is_visible)
        return False

    def _set_cell_text_name(self, column, cell, model, iter_):
        name = model.get_value(iter_, 1)
        is_stopped = model.get_value(iter_, 2)
        cell.set_property("sensitive", not is_stopped)
        cell.set_property("text", name)

    def _set_cell_pixbuf_state(self, column, cell, model, iter_, index):
        state_info = model.get_value(iter_, index)
        if state_info is None:
            cell.set_property("pixbuf", None)
            cell.set_property("visible", False)
            return
        state, num_tasks = state_info.rsplit(" ", 1)
        icon = self.dots.get_icon(state)
        cell.set_property("pixbuf", icon)
        cell.set_property("visible", True)

    def _set_cell_text_time(self, column, cell, model, iter_):
        update_time = model.get_value(iter_, 3)
        time_object = datetime.datetime.fromtimestamp(update_time)
        time_string = time_object.strftime("%dT%H:%M")
        cell.set_property("text", time_string)

    def _set_theme(self, theme_menu_item):
        if not theme_menu_item.get_active():
            return False
        self.theme_name = theme_menu_item.theme_name
        self.theme = self.usercfg['themes'][self.theme_name]
        self.dots = DotMaker(self.theme)

    def _set_tooltip(self, widget, text):
        tooltip = gtk.Tooltips()
        tooltip.enable()
        tooltip.set_tip(widget, text)


class SummaryAppUpdater(threading.Thread):

    """Update the summary app."""
    
    POLL_INTERVAL = 20
    STOPPED_SUITE_CLEAR_TIME = 86400
    
    def __init__(self, hosts, suite_treemodel, owner=None):
        self.hosts = hosts
        if owner is None:
           owner = user
        self.owner = owner
        self.suite_treemodel = suite_treemodel
        self.stop_summaries = {}
        self.quit = False
        super(SummaryAppUpdater, self).__init__()

    def clear_stopped_suites(self):
        self.stop_summaries.clear()

    def run(self):
        all_host_suites = []
        prev_suites = {}
        last_update_time = None
        while not self.quit:
            current_time = time.time()
            if (last_update_time is not None and
                current_time < last_update_time + self.POLL_INTERVAL):
                time.sleep(1)
                continue
            host_suites = get_host_suites(self.hosts)
            statuses = {}
            current_suites = []
            for host in self.hosts:
                for suite in host_suites[host]:
                    status_tasks = get_status_tasks(host, suite)
                    statuses.setdefault(host, {})
                    statuses[host].setdefault(suite, {})
                    statuses[host][suite] = status_tasks
                    if (host, suite) in self.stop_summaries:
                        self.stop_summaries.pop((host, suite))
                    current_suites.append((host, suite))
            for host, suite in prev_suites:
                if (host, suite) not in current_suites:
                    self.stop_summaries.setdefault(host, {})
                    summary_statuses = get_status_tasks(host, suite)
                    self.stop_summaries[host][suite] = (summary_statuses,
                                                        current_time)
            prev_suites = copy.deepcopy(current_suites)
            for host in self.stop_summaries:
                for suite in self.stop_summaries[host].keys():
                    if (self.stop_summaries[host][suite][1] +
                        self.STOPPED_SUITE_CLEAR_TIME < current_time):
                        self.stop_summaries[host].pop(suite)
            last_update_time = time.time()
            gobject.idle_add(self.update, statuses,
                             self.stop_summaries, current_time)
            time.sleep(1)

    def update(self, host_suite_statuses, host_suite_stop_summaries,
               update_time):
        self.suite_treemodel.clear()
        for host in self.hosts:
            suites = sorted(host_suite_statuses.get(host, {}).keys() +
                            host_suite_stop_summaries.get(host, {}).keys())
            for suite in suites:
                if suite in host_suite_statuses.get(host, {}):
                    statuses = host_suite_statuses[host][suite].items()
                    is_stopped = False
                    suite_time = update_time
                else:
                    info = host_suite_stop_summaries[host][suite]
                    status_map, suite_time = info
                    statuses = status_map.items()
                    is_stopped = True
                statuses.sort()
                statuses.sort(lambda x, y: cmp(len(x[1]), len(y[1])))
                states = [s[0] + " " + str(len(s[1])) for s in statuses]
                model_data = [host, suite, is_stopped, suite_time]
                model_data += states[:20]
                model_data += [None] * (24 - len(model_data))
                self.suite_treemodel.append(None, model_data)
        return False
