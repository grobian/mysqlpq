mysqlpq
=======

A proxy for MySQL aimed at targetting multiple MySQL instances.

This project aims to provide a single entry point for clients in (table)
sharded MySQL setups.  The Parallel Query (pq) proxy works by returning
the first or all answers combined from a set of servers for each query
it gets.  This can be used for performance cases, or (as is intended)
for use with sharded setups where a database (schema), table or row
lives only on one of the upstream MySQL servers.  In this case, mysqlpq
will return the (non-error) answer from the MySQL server that replies to
the query or the combined resultset of all.

mysqlpq is currently being tested and under development.  It is pure
alpha software, and its features and purposes may change considerably in
the future.  You should not use this software, if you do, remember that
patches are welcome :)


Author
------
Fabian Groffen


Acknowledgement
---------------
This program was originally developed for Booking.com.  With approval
from Booking.com, the code was generalised and published as Open Source
on github, for which the author would like to express his gratitude.
