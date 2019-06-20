#!/usr/bin/env python

import sqlite3
import datetime
from collections import defaultdict, namedtuple

# define some classes for data records
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

class UserToolSummary:
    __slots__ = ['name', 'logins', 'total_time']
    def __init__(self, name='', logins=0, total_time=datetime.timedelta()):
        self.name = ''
        self.logins = logins
        self.total_time = total_time

    def __repr__(self):
        return "UserToolSummary(logins={}, total_time={})".format(
                self.logins, self.total_time)

    def __lt__(self, other):
        return self.total_time < other.total_time

# connect to DB, get tool and user names
db = sqlite3.connect('db.db')

tools = db.execute("SELECT id, name FROM device")
toolnames = {}
for tool in tools:
    toolnames[str(tool[0])] = tool[1]

users = db.execute("SELECT id, name, code FROM user")
user_id_to_name = {}
user_code_to_id = {}
for user in users:
    user_id_to_name[str(user[0])] = str(user[1])
    user_code_to_id[str(user[2])] = str(user[0])

# generate summaries
msgs = db.execute("SELECT message, Timestamp FROM log WHERE Timestamp BETWEEN ? AND ?", ('2019-05-01', '2019-05-31'))

summaries = defaultdict(ToolSummary)
states = defaultdict(ToolState)
user_summaries = defaultdict(UserToolSummary)

other_msgs = 0;

unmatched = 0
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
            print('Login without logout: ', ts, toolnames[tool],
                    user_id_to_name[user], states[tool].login_time)
        states[tool].in_use = True
        states[tool].active_user = user
        states[tool].login_time = ts
        user_summaries[(user, tool)].logins += 1
        user_summaries[(user, tool)].name = user_id_to_name[user]
    elif fields[0] == 'logout':
        user = user_code_to_id[user]
        summaries[tool].logouts += 1
        if not states[tool].in_use:
            print('Logout without login: ', ts, toolnames[tool],
                    user_id_to_name[user])
        else:
            states[tool].in_use = False
            summaries[tool].total_time += (ts - states[tool].login_time)
            if states[tool].active_user == user:
                user_summaries[(user, tool)].total_time += (ts - states[tool].login_time)
            else:
                unmatched += 1

leaderboards = defaultdict(list)
for ((_, tool), s) in user_summaries.items():
    leaderboards[tool].append(s)

print('non-login/logout messages:', other_msgs)
print('unmatched login/logout pairs:', unmatched)

for (tool, s) in summaries.items():
    print()
    print('Tool:', toolnames[tool])
    print('  Logins: ', s.logins)
    print('  Logouts:', s.logouts)
    print('  Total logged time:', s.total_time)
    print('  Leaderboard:')
    leaderboards[tool].sort()
    for s in list(reversed(leaderboards[tool]))[:10]:
        print('   ', s.name, s.total_time)
