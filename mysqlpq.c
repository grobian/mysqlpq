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

int keep_running = 1;
char mysqlpq_hostname[128];


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
	printf("  -f  read <config> for clusters and routes\n");
	printf("  -p  listen on <port> for connections, defaults to 3306\n");
	printf("  -w  user <workers> worker threads, defaults to 16\n");
	printf("  -b  server send batch size, defaults to 2500\n");
	printf("  -q  server queue size, defaults to 25000\n");
	printf("  -d  debug mode: currently writes statistics to stdout\n");
	printf("  -H  hostname: override hostname (used in statistics)\n");

	exit(exitcode);
}

enum rmode {
	mNORMAL,
	mDEBUG
};

int
main(int argc, char * const argv[])
{
	int sock[] = {0, 0, 0, 0, 0};  /* tcp4, udp4, tcp6, udp6, UNIX */
	int socklen = sizeof(sock) / sizeof(sock[0]);
	char id;
	dispatcher **workers;
	char workercnt = 0;
	char *routes = NULL;
	unsigned short listenport = 3306;
	int batchsize = 2500;
	int queuesize = 25000;
	enum rmode mode = mNORMAL;
	int ch;
	char nowbuf[24];

	if (gethostname(mysqlpq_hostname, sizeof(mysqlpq_hostname)) < 0)
		snprintf(mysqlpq_hostname, sizeof(mysqlpq_hostname), "127.0.0.1");

	while ((ch = getopt(argc, argv, ":hvdstf:p:w:b:q:H:")) != -1) {
		switch (ch) {
			case 'v':
				do_version();
				break;
			case 'd':
				mode = mDEBUG;
				break;
			case 'f':
				routes = optarg;
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
			case 'b':
				batchsize = atoi(optarg);
				if (batchsize <= 0) {
					fprintf(stderr, "error: batch size needs to be a number >0\n");
					do_usage(1);
				}
				break;
			case 'q':
				queuesize = atoi(optarg);
				if (queuesize <= 0) {
					fprintf(stderr, "error: queue size needs to be a number >0\n");
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
	if (optind == 1 || routes == NULL)
		do_usage(1);


	/* seed randomiser for dispatcher and aggregator "splay" */
	srand(time(NULL));

	if (workercnt == 0)
		workercnt = 16;

	/* any_of failover maths need batchsize to be smaller than queuesize */
	if (batchsize > queuesize) {
		fprintf(stderr, "error: batchsize must be smaller than queuesize\n");
		exit(-1);
	}

	fprintf(stdout, "[%s] starting mysqlpq v%s (%s)\n",
		fmtnow(nowbuf), VERSION, GIT_VERSION);
	fprintf(stdout, "configuration:\n");
	fprintf(stdout, "    mysqlpq hostname = %s\n", mysqlpq_hostname);
	fprintf(stdout, "    listen port = %u\n", listenport);
	fprintf(stdout, "    workers = %d\n", workercnt);
	fprintf(stdout, "    send batch size = %d\n", batchsize);
	fprintf(stdout, "    server queue size = %d\n", queuesize);
	if (mode == mDEBUG)
		fprintf(stdout, "    debug = true\n");
	fprintf(stdout, "    routes configuration = %s\n", routes);
	fprintf(stdout, "\n");

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
//	collector_start((void **)&workers[1], (void **)servers, mode,
//			internal_submission);

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
//	collector_stop();
//	fprintf(stdout, "[%s] collector stopped\n", fmtnow(nowbuf));
//	fflush(stdout);
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

	fprintf(stdout, "[%s] routing stopped\n", fmtnow(nowbuf));
	fflush(stdout);

	fflush(stderr);  /* ensure all of our termination messages are out */

	free(workers);
	return 0;
}
