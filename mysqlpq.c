/*
 * Copyright 2013-2014 Fabian Groffen
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
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

#include "mysqlpq.h"
#include "receptor.h"
#include "dispatcher.h"
#include "collector.h"

int keep_running = 1;
char mysqlpq_hostname[128];
char *connect_username = NULL;
char *connect_passwd = NULL;
char **connect_hosts = NULL;
int connect_host_cnt = 0;
int startuptime = 0;


static void
exit_handler(int sig)
{
	char *signal = "unknown signal";

	switch (sig) {
		case SIGTERM:
			signal = "SIGTERM";
			break;
		case SIGINT:
			signal = "SIGINT";
			break;
		case SIGQUIT:
			signal = "SIGQUIT";
			break;
	}
	if (keep_running) {
		fprintf(stdout, "caught %s, terminating...\n", signal);
		fflush(stdout);
	} else {
		fprintf(stderr, "caught %s while already shutting down, "
				"forcing exit...\n", signal);
		fflush(NULL);
		exit(1);
	}
	keep_running = 0;
}

char *
fmtnow(char nowbuf[24])
{
	time_t now;
	struct tm *tm_now;

	time(&now);
	tm_now = localtime(&now);
	strftime(nowbuf, 24, "%Y-%m-%d %H:%M:%S", tm_now);

	return nowbuf;
}

void
do_version(void)
{
	printf("mysqlpq v" VERSION " (" GIT_VERSION ")\n");

	exit(0);
}

void
do_usage(int exitcode)
{
	printf("Usage: mysqlpq [-vdst] -f <config> [-p <port>] [-w <workers>] [-b <size>] [-q <size>]\n");
	printf("\n");
	printf("Options:\n");
	printf("  -v  print version and exit\n");
	printf("  -f  read <config> for backends\n");
	printf("  -p  listen on <port> for connections, defaults to 3306\n");
	printf("  -w  use <workers> worker threads, defaults to 16\n");
	printf("  -d  debug mode: currently writes statistics to stdout\n");
	printf("  -H  hostname: override hostname (used in statistics)\n");

	exit(exitcode);
}

int
main(int argc, char * const argv[])
{
	int sock[] = {0, 0, 0, 0, 0};  /* tcp4, udp4, tcp6, udp6, UNIX */
	int socklen = sizeof(sock) / sizeof(sock[0]);
	char id;
	dispatcher **workers;
	char workercnt = 0;
	char *config = NULL;
	unsigned short listenport = 3306;
	char mode = 0;
	int ch;
	char nowbuf[24];
	time_t now;

	if (gethostname(mysqlpq_hostname, sizeof(mysqlpq_hostname)) < 0)
		snprintf(mysqlpq_hostname, sizeof(mysqlpq_hostname), "127.0.0.1");

	while ((ch = getopt(argc, argv, ":hvdstf:p:w:b:q:H:")) != -1) {
		switch (ch) {
			case 'v':
				do_version();
				break;
			case 'd':
				mode = 1;
				break;
			case 'f':
				config = optarg;
				break;
			case 'p':
				listenport = (unsigned short)atoi(optarg);
				if (listenport == 0) {
					fprintf(stderr, "error: port needs to be a number >0\n");
					do_usage(1);
				}
				break;
			case 'w':
				workercnt = (char)atoi(optarg);
				if (workercnt <= 0) {
					fprintf(stderr, "error: workers needs to be a number >0\n");
					do_usage(1);
				}
				break;
			case 'H':
				snprintf(mysqlpq_hostname, sizeof(mysqlpq_hostname), "%s", optarg);
				break;
			case '?':
			case ':':
				do_usage(1);
				break;
			case 'h':
			default:
				do_usage(0);
				break;
		}
	}
	if (optind == 1 || config == NULL)
		do_usage(1);


	/* seed randomiser for dispatcher and aggregator "splay" */
	srand(time(NULL));

	time(&now);
	startuptime = (int)now;

	if (workercnt == 0)
		workercnt = 16;

	fprintf(stdout, "[%s] starting mysqlpq v%s (%s)\n",
		fmtnow(nowbuf), VERSION, GIT_VERSION);
	fprintf(stdout, "configuration:\n");
	fprintf(stdout, "    mysqlpq hostname = %s\n", mysqlpq_hostname);
	fprintf(stdout, "    listen port = %u\n", listenport);
	fprintf(stdout, "    workers = %d\n", workercnt);
	if (mode)
		fprintf(stdout, "    debug = true\n");
	fprintf(stdout, "    configuration = %s\n", config);
	fprintf(stdout, "\n");

	/* lame config reading */
	{
		FILE *c = fopen(config, "r");
		char buf[256];
		char *p;
		if (c == NULL) {
			fprintf(stderr, "failed to open %s: %s\n", config, strerror(errno));
			return 1;
		}
		if (fgets(buf, sizeof(buf), c) != NULL) {
			for (p = buf; *p != '\0'; p++)
				if (*p == '\n') {
					*p = '\0';
					break;
				}
			connect_username = strdup(buf);
		}
		if (fgets(buf, sizeof(buf), c) != NULL) {
			for (p = buf; *p != '\0'; p++)
				if (*p == '\n') {
					*p = '\0';
					break;
				}
			connect_passwd = strdup(buf);
		}
		if (fgets(buf, sizeof(buf), c) != NULL) {
			int i = 1;
			char *q;
			char **hosts;
			for (p = buf; *p != '\0'; p++)
				if (*p == ',')
					i++;
			hosts = connect_hosts = malloc(sizeof(char *) * i);
			connect_host_cnt = i;
			for (p = buf, q = buf; *p != '\0'; p++)
				if (*p == ',') {
					*p = '\0';
					*hosts++ = strdup(q);
					q = p + 1;
				} else if (*p == '\n') {
					*p = '\0';
				}
			*hosts = strdup(q);
		}
	}

	if (signal(SIGINT, exit_handler) == SIG_ERR) {
		fprintf(stderr, "failed to create SIGINT handler: %s\n",
				strerror(errno));
		return 1;
	}
	if (signal(SIGTERM, exit_handler) == SIG_ERR) {
		fprintf(stderr, "failed to create SIGTERM handler: %s\n",
				strerror(errno));
		return 1;
	}
	if (signal(SIGQUIT, exit_handler) == SIG_ERR) {
		fprintf(stderr, "failed to create SIGQUIT handler: %s\n",
				strerror(errno));
		return 1;
	}
	if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
		fprintf(stderr, "failed to ignore SIGPIPE: %s\n",
				strerror(errno));
		return 1;
	}

	workers = malloc(sizeof(dispatcher *) * (1 + workercnt + 1));
	if (workers == NULL) {
		fprintf(stderr, "failed to allocate memory for workers\n");
		return 1;
	}

	if (bindlisten(sock, &socklen, listenport) < 0) {
		fprintf(stderr, "failed to bind on port %d: %s\n",
				listenport, strerror(errno));
		return -1;
	}
	for (ch = 0; ch < socklen; ch++) {
		if (dispatch_addlistener(sock[ch]) != 0) {
			fprintf(stderr, "failed to add listener\n");
			return -1;
		}
	}
	if ((workers[0] = dispatch_new_listener()) == NULL)
		fprintf(stderr, "failed to add listener\n");

	fprintf(stdout, "starting %d workers\n", workercnt);
	for (id = 1; id < 1 + workercnt; id++) {
		workers[id + 0] = dispatch_new_connection();
		if (workers[id + 0] == NULL) {
			fprintf(stderr, "failed to add worker %d\n", id);
			break;
		}
	}
	workers[id + 0] = NULL;
	if (id < 1 + workercnt) {
		fprintf(stderr, "shutting down due to errors\n");
		keep_running = 0;
	}

	fprintf(stdout, "starting statistics collector\n");
	collector_start(&workers[1], mode, NULL);

	fflush(stdout);  /* ensure all info stuff up here is out of the door */

	/* workers do the work, just wait */
	while (keep_running)
		sleep(1);

	fprintf(stdout, "[%s] shutting down...\n", fmtnow(nowbuf));
	fflush(stdout);
	/* make sure we don't accept anything new anymore */
	for (ch = 0; ch < socklen; ch++)
		dispatch_removelistener(sock[ch]);
	destroy_usock(listenport);
	fprintf(stdout, "[%s] listener for port %u closed\n",
			fmtnow(nowbuf), listenport);
	fflush(stdout);
	/* since workers will be freed, stop querying the structures */
	collector_stop();
	fprintf(stdout, "[%s] collector stopped\n", fmtnow(nowbuf));
	fflush(stdout);
	/* give a little time for whatever the collector/aggregator wrote,
	 * to be delivered by the dispatchers */
	usleep(500 * 1000);  /* 500ms */
	/* make sure we don't write to our servers any more */
	fprintf(stdout, "[%s] stopped worker", fmtnow(nowbuf));
	fflush(stdout);
	for (id = 0; id < 1 + workercnt; id++)
		dispatch_stop(workers[id + 0]);
	for (id = 0; id < 1 + workercnt; id++) {
		dispatch_shutdown(workers[id + 0]);
		fprintf(stdout, " %d", id + 1);
		fflush(stdout);
	}
	fprintf(stdout, " (%s)\n", fmtnow(nowbuf));
	fflush(stdout);

	fprintf(stdout, "[%s] proxy stopped\n", fmtnow(nowbuf));
	fflush(stdout);

	fflush(stderr);  /* ensure all of our termination messages are out */

	free(workers);
	return 0;
}
