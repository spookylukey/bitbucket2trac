#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Import Bitbucket exported issues into a Trac database.
"""
# Originally based on code from https://github.com/trac-hacks/mantis2trac, but
# largely rewritten. For unimplemented things that source may be helpful.

from __future__ import absolute_import, print_function

import argparse
import calendar
import json

import dateutil.parser
from trac.env import Environment

# -- Constants

# Enums: as much as possible, we try to use Trac's defaults, and map BitBucket
# to these, for maximum compat with an out-of-the-box Trac installation. But
# where there isn't an exact match, we add to Trac's values.

# Trac's defaults below, others are added automatically.
PRIORITY_LIST = [
    ('blocker', 1),
    ('critical', 2),
    ('major', 3),
    ('minor', 4),
    ('trivial', 5),
]

RESOLUTION_LIST = [
    ('fixed', 1),
    ('invalid', 2),
    ('wontfix', 3),
    ('duplicate', 4),
    ('worksforme', 5),
]

TYPE_LIST = [
    ('defect', 1),
    ('enhancement', 2),
    ('task', 3),
]

# Maps from BitBucket to the above lists, where necessary:
TYPE_MAP = {
    'bug': 'defect',
}

PRIORITY_MAP = {
}

RESOLUTION_MAP = {
}

# For bugs with no component set.
DEFAULT_COMPONENT = 'web'

# -- End config

parser = argparse.ArgumentParser()
parser.add_argument('source', help='BitBucket JSON file')
parser.add_argument('tracenv', help='Trac environment to import into')
parser.add_argument('--clean', action='store_true', help='Clean out all issues etc. in Trac first')


class TracDatabase(object):
    def __init__(self, path):
        self.env = Environment(path)

    def execute(self, sql, params=()):
        with self.env.db_transaction as db:
            c = db.cursor()
            c.execute(sql, params)
            return c.fetchall()

    def has_tickets(self):
        return self.execute('''SELECT count(*) FROM ticket''')[0][0] > 0

    def assert_no_tickets(self):
        if self.has_tickets():
            raise Exception("Will not modify database with existing tickets!")
        return

    def set_severity_list(self, s):
        """Remove all severities, set them to `s`"""
        self.assert_no_tickets()

        self.execute("""DELETE FROM enum WHERE type='severity'""")
        for value, i in s:
            print("inserting severity ", value, " ", i)
            self.execute("""INSERT INTO enum (type, name, value) VALUES (%s, %s, %s)""",
                         ["severity", value, i])

    def set_priority_list(self, s):
        """Remove all priorities, set them to `s`"""
        self.assert_no_tickets()

        self.execute("""DELETE FROM enum WHERE type='priority'""")
        for value, i in s:
            print("inserting priority ", value, " ", i)
            self.execute("""INSERT INTO enum (type, name, value) VALUES (%s, %s, %s)""",
                         ["priority", value, i])

    def set_resolution_list(self, s):
        self.assert_no_tickets()

        self.execute("""DELETE FROM enum WHERE type='resolution'""")
        for value, i in s:
            print("inserting resolution ", value, " ", i)
            self.execute("""INSERT INTO enum (type, name, value) VALUES (%s, %s, %s)""",
                         ["resolution", value, i])

    def set_type_list(self, s):
        self.assert_no_tickets()

        self.execute("""DELETE FROM enum WHERE type='ticket_type'""")
        for value, i in s:
            print("inserting ticket_type ", value, " ", i)
            self.execute("""INSERT INTO enum (type, name, value) VALUES (%s, %s, %s)""",
                         ["ticket_type", value, i])

    def set_component_list(self, l):
        """Remove all components, set them to `l`"""
        self.assert_no_tickets()

        self.execute("""DELETE FROM component""")
        for name, owner in l:
            print("inserting component %s (%s)" % (name, owner))
            self.execute("""INSERT INTO component (name, owner) VALUES (%s, %s)""",
                         [name, owner])

    def add_ticket(self, id, time, changetime, ticket_type, component,
                   severity, priority, owner, reporter, cc,
                   version, milestone, status, resolution,
                   summary, description):
        print("inserting ticket %s -- \"%s\"" % (id, summary[0:40].replace("\n", " ")))
        with self.env.db_transaction as db:
            c = db.cursor()
            c.execute("""INSERT INTO ticket (id, type, time, changetime, component,
                                         severity, priority, owner, reporter, cc,
                                         version, milestone, status, resolution,
                                         summary, description)
                                 VALUES (%s, %s, %s, %s, %s,
                                         %s, %s, %s, %s, %s,
                                         %s, %s, %s, %s,
                                         %s, %s)""",
                      (id,
                       ticket_type,
                       self.convert_datetime(time),
                       self.convert_datetime(changetime),
                       component,
                       severity,
                       priority,
                       owner,
                       reporter,
                       cc,
                       version,
                       milestone,
                       status.lower(),
                       resolution,
                       summary,
                       description))
            return db.get_last_id(c, 'ticket')

    def convert_datetime(self, dt):
        return calendar.timegm(dt.utctimetuple()) * 1000000

    def add_ticket_comment(self, ticket, time, author, value):
        print("adding comment \"%s...\"" % value[0:40].replace("\n", " "))
        comment = value
        self.execute("""INSERT  INTO ticket_change (ticket, time, author, field, oldvalue, newvalue)
                        VALUES        (%s, %s, %s, %s, %s, %s)""",
                     [ticket, self.convert_datetime(time), author, 'comment', '', comment])

    def add_ticket_change(self, ticket, time, author, field, oldvalue, newvalue):
        print("adding ticket change \"%s\": %r -> %r (%s)" % (
            field,
            oldvalue[0:20].replace("\n", " ") if oldvalue else oldvalue,
            newvalue[0:20].replace("\n", " ") if newvalue else newvalue,
            time)
        )

        self.execute("""INSERT  INTO ticket_change (ticket, time, author, field, oldvalue, newvalue)
                                 VALUES        (%s, %s, %s, %s, %s, %s)""",
                     (ticket, self.convert_datetime(time), author, field, oldvalue, newvalue))


def add_enums(base, extras):
    retval = base[:]
    initial = [k for k, i in retval]
    retval.extend([
        (k, i) for i, k in enumerate([
            extra for extra in set(extras)
            if extra not in initial
        ], max([i for k, i in retval]) + 1)
    ])
    return retval


def map_priority(priority):
    return PRIORITY_MAP.get(priority, priority)


def map_ticket_type(kind):
    return TYPE_MAP.get(kind, kind)


def status_to_trac(status):
    if status in ['closed', 'resolved']:
        trac_status = 'closed'
        trac_resolution = 'fixed'
    elif status == 'new':
        trac_status = 'new'
        trac_resolution = None
    elif status == 'open':
        trac_status = 'accepted'
        trac_resolution = None
    else:
        trac_status = 'closed'
        trac_resolution = RESOLUTION_MAP.get(status, status)
    return trac_status, trac_resolution


def no_op(val):
    return val


# For 'logs' (i.e. changes), map 'field' to Trac field
FIELD_MAP = {
    'component': ('component', no_op),
    'content': ('description', no_op),
    'kind': ('ticket_type', map_ticket_type),
    'priority': ('priority', map_priority),
    'responsible': ('owner', no_op),
    'title': ('summary', no_op),
    # status is more complex, handled below
}


def main(source, trac_env, clean=True, default_user='nobody'):
    source_data = json.load(open(source))
    issues = source_data['issues']

    # init Trac environment
    trac = TracDatabase(trac_env)

    if clean:
        print("## Cleaning all tickets...")
        trac.execute("DELETE FROM ticket_change")
        trac.execute("DELETE FROM ticket")
        trac.execute("DELETE FROM ticket_custom")
        trac.execute("DELETE FROM attachment")

    print("# Importing priorities...")
    priority_list = add_enums(
        PRIORITY_LIST,
        [issue["priority"] for issue in issues
         if issue["priority"] not in PRIORITY_MAP]
    )
    trac.set_priority_list(priority_list)

    print("# Importing  resolutions...")
    extra_resolutions = [
        r
        for s, r in
        [status_to_trac(issue["status"]) for issue in issues]
        if s == 'closed' and r not in RESOLUTION_MAP
    ]
    resolution_list = add_enums(RESOLUTION_LIST, extra_resolutions)
    trac.set_resolution_list(resolution_list)

    print("# Importing  types...")
    type_list = add_enums(
        TYPE_LIST,
        [
            issue["kind"] for issue in issues
            if issue["kind"] not in TYPE_MAP
        ]
    )
    trac.set_type_list(type_list)

    print("# Importing components...")
    components = sorted(list(set(i['component'] for i in issues) | set([DEFAULT_COMPONENT])))
    trac.set_component_list([(c, default_user) for c in components if c])

    # TODO - versions
    # TODO - milestones

    print('# Importing issues')
    for issue in issues:
        status = issue['status']
        trac_status, trac_resolution = status_to_trac(status)
        trac.add_ticket(
            id=issue['id'],
            time=dateutil.parser.parse(issue['created_on']),
            changetime=dateutil.parser.parse(issue['updated_on']),
            ticket_type=map_ticket_type(issue['kind']),
            component=issue['component'] or DEFAULT_COMPONENT,
            severity=None,
            priority=map_priority(issue['priority']),
            owner=issue['assignee'],
            reporter=issue['reporter'],
            cc='',
            version=issue['version'],
            milestone=issue['milestone'],
            status=trac_status,
            resolution=trac_resolution,
            summary=issue['title'],
            description=issue['content'],
        )

    print("# Importing issue comments")
    for comment in source_data['comments']:
        trac.add_ticket_comment(
            comment['issue'],
            dateutil.parser.parse(comment['created_on']),
            comment['user'],
            comment['content'] or '',
        )

    print("# Importing issue changes")
    for log in source_data['logs']:
        changes = []
        field = log['field']
        old_value = log['changed_from']
        new_value = log['changed_to']
        if field in FIELD_MAP:
            mapped_field, fixer = FIELD_MAP[field]
            changes.append((mapped_field, fixer(old_value), fixer(new_value)))
        elif field == 'status':
            old_trac_status, old_trac_resolution = status_to_trac(old_value)
            new_trac_status, new_trac_resolution = status_to_trac(new_value)
            changes.append(('status', old_trac_status, new_trac_status))
            changes.append(('resolution', old_trac_resolution, new_trac_resolution))
        else:
            raise NotImplementedError(field)

        for field, old_value, new_value in changes:
            trac.add_ticket_change(
                log['issue'],
                dateutil.parser.parse(log['created_on']),
                log['user'],
                field,
                old_value,
                new_value,
            )

    # TODO attachments


if __name__ == '__main__':
    args = parser.parse_args()
    main(args.source, args.tracenv, clean=args.clean)
