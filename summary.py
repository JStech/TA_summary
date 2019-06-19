#!/usr/bin/env python

import sqlite3
import datetime
from collections import defaultdict, namedtuple

class ToolSummary:
    __slots__ = ['logins', 'logouts', 'total_time']
    def __init__(self, logins=0, logouts=0, total_time=datetime.timedelta()):
        self.logins = logins
        self.logouts = logouts
        self.total_time = total_time

    def __repr__(self):
        return "ToolSummary(logins={}, logouts={}, total_time={})".format(
                self.logins, self.logouts, self.total_time)

class ToolState:
    __slots__ = ['in_use', 'active_user', 'login_time']
    def __init__(self, in_use=False, active_user=0, login_time=0):
        self.in_use = in_use
        self.active_user = active_user
        self.login_time = login_time

db = sqlite3.connect('db.db')

tools = db.execute("SELECT id, name FROM device")
toolnames = {}
for tool in tools:
    toolnames[str(tool[0])] = tool[1]

msgs = db.execute("SELECT message, Timestamp FROM log WHERE Timestamp BETWEEN ? AND ?", ('2019-05-01', '2019-05-31'))

summaries = defaultdict(ToolSummary)
states = defaultdict(ToolState)
other_msgs = 0;

for msg in msgs:
    ts = datetime.datetime.strptime(msg[1], '%Y-%m-%d %H:%M:%S')
    fields = msg[0].split(':')
    if (len(fields) != 3) or (fields[0] not in ('login', 'logout')):
        other_msgs += 1
        continue

    tool = fields[1]
    user = fields[2]
    if fields[0] == 'login':
        summaries[tool].logins += 1
        if states[tool].in_use:
            print('Login without logout: ', ts, toolnames[tool], user, states[tool].login_time)
        states[tool].in_use = True
        states[tool].active_user = user
        states[tool].login_time = ts
    elif fields[0] == 'logout':
        summaries[tool].logouts += 1
        if not states[tool].in_use:
            print('Logout without login: ', ts, toolnames[tool], user)
        else:
            states[tool].in_use = False
            summaries[tool].total_time += (ts - states[tool].login_time)

print('non-login/logout messages:', other_msgs)
for (tool, s) in summaries.items():
    print('Tool:', toolnames[tool])
    print('  Logins: ', s.logins)
    print('  Logouts:', s.logouts)
    print('  Total logged time:', s.total_time)
