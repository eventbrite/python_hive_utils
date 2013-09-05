==========
hive_utils
==========

Utilities for accessing and using hive with python.

Installing
==========

You can install **hive_utils** through ``pip`` or ``easy-install``::

    pip install hive_utils

Or you can download the `latest development version`_, which may
contain new features.

Using hive_utils
================

**hive_utils** allows you to access hive with an API that is similar to
MySQL-python::

    query = """
        SELECT country, count(1) AS cnt
        FROM User
        GROUP BY country
    """
    hive_client = hive_utils.HiveClient(
        server=config['HOST'],
        port=config['PORT'],
        db=config['NAME'],
    )
    for row in hive_client.execute(query):
        print '%s: %s' % (row['country'], row['cnt'])

License
========

**hive_utils** is copyright 2013 Eventbrite and Contributors, and is made
available under BSD-style license; see LICENSE for details.

.. _`latest development version`: https://github.com/eventbrite/python_hive_utils/tarball/master#egg=hive_utils
