/*
 * Copyright 2013-2015 Fabian Groffen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <pthread.h>
#include <errno.h>

#include "mysqlpq.h"
#include "dispatcher.h"
#include "collector.h"

static dispatcher **dispatchers;
static char debug = 0;
static pthread_t collectorid;
static char keep_running = 1;
int collector_interval = 60;

/**
 * Collects metrics from dispatchers and servers and emits them.
 */
static void *
collector_runner(void *s)
{
	int i;
	size_t totticks;
	size_t totqueries;
	size_t totacceptedconns;
	size_t totclosedconns;
	size_t ticks;
	size_t queries;
	size_t dispatchers_idle;
	size_t dispatchers_busy;
	time_t now;
	time_t nextcycle;
	char metric[2048];
	char *m;
	size_t sizem = 0;
	int sock;
	char use_udp = 0;
	char *ip;
	char sport[16];
	struct addrinfo hint;
	struct addrinfo *saddrs;

	/* prepare hostname for graphite metrics */
	snprintf(metric, sizeof(metric), "mysql.pq.%s", mysqlpq_hostname);
	for (m = metric + strlen("mysql.pq."); *m != '\0'; m++)
		if (*m == '.')
			*m = '_';
	*m++ = '.';
	*m = '\0';
	sizem = sizeof(metric) - (m - metric);

#define send(metric) \
	if (debug) \
		fprintf(stdout, "%s", metric); \
	else \
		write(sock, metric, strlen(metric));

	nextcycle = time(NULL) + collector_interval;
	while (keep_running) {
		sleep(1);
		now = time(NULL);
		if (nextcycle > now)
			continue;
		nextcycle += collector_interval;

		/* resolve + open connection */
		memset(&hint, 0, sizeof(hint));
		use_udp = 1;
		hint.ai_family = PF_UNSPEC;
		hint.ai_socktype = use_udp ? SOCK_DGRAM : SOCK_STREAM;
		hint.ai_protocol = use_udp ? IPPROTO_UDP : IPPROTO_TCP;
		hint.ai_flags = AI_NUMERICSERV;
		snprintf(sport, sizeof(sport), "%u", 3002);  /* for default */
		ip = "127.0.0.1";

		if ((i = getaddrinfo(ip, sport, &hint, &saddrs)) != 0) {
			fprintf(stderr, "failed to resolve server %s:%s (%s): %s\n",
					ip, sport, use_udp ? "udp" : "tcp", gai_strerror(i));
			continue;
		}
		if ((sock = socket(saddrs->ai_family,
						saddrs->ai_socktype,
						saddrs->ai_protocol)) < 0)
		{
			fprintf(stderr, "failed to create socket: %s\n", strerror(errno));
			freeaddrinfo(saddrs);
			continue;
		}
		if (connect(sock, saddrs->ai_addr, saddrs->ai_addrlen) < 0) {
			fprintf(stderr, "failed to connect socket: %s\n", strerror(errno));
			close(sock);
			freeaddrinfo(saddrs);
			continue;
		}
		freeaddrinfo(saddrs);

		totticks = 0;
		totqueries = 0;
		dispatchers_idle = 0;
		dispatchers_busy = 0;
		for (i = 0; dispatchers[i] != NULL; i++) {
			if (dispatch_busy(dispatchers[i])) {
				dispatchers_busy++;
			} else {
				dispatchers_idle++;
			}
			totticks += ticks = dispatch_get_ticks(dispatchers[i]);
			totqueries += queries = dispatch_get_queries(dispatchers[i]);
			snprintf(m, sizem, "dispatcher%d.queriesReceived %zd %zd\n",
					i + 1, queries, (size_t)now);
			send(metric);
			snprintf(m, sizem, "dispatcher%d.wallTime_us %zd %zd\n",
					i + 1, ticks, (size_t)now);
			send(metric);
		}
		snprintf(m, sizem, "queriesReceived %zd %zd\n",
				totqueries, (size_t)now);
		send(metric);
		snprintf(m, sizem, "dispatch_wallTime_us %zd %zd\n",
				totticks, (size_t)now);
		send(metric);
		totacceptedconns = dispatch_get_accepted_connections();
		totclosedconns = dispatch_get_closed_connections();
		snprintf(m, sizem, "acceptedConnections %zd %zd\n",
				totacceptedconns, (size_t)now);
		send(metric);
		snprintf(m, sizem, "closedConnections %zd %zd\n",
				totclosedconns, (size_t)now);
		send(metric);
		snprintf(m, sizem, "dispatch_busy %zd %zd\n",
				dispatchers_busy, (size_t)now);
		send(metric);
		snprintf(m, sizem, "dispatch_idle %zd %zd\n",
				dispatchers_idle, (size_t)now);
		send(metric);

		if (debug) {
			fflush(stdout);
		} else {
			close(sock);
		}
	}

	return NULL;
}

/**
 * Initialises and starts the collector.
 */
void
collector_start(dispatcher **d, char dbg, void *s)
{
	dispatchers = d;

	if (dbg)
		debug = 1;

	if (pthread_create(&collectorid, NULL, collector_runner, s) != 0)
		fprintf(stderr, "failed to start collector!\n");
}

/**
 * Shuts down the collector.
 */
void
collector_stop(void)
{
	keep_running = 0;
	pthread_join(collectorid, NULL);
}
