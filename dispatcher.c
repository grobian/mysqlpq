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
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <assert.h>

#include "mysqlpq.h"
#include "collector.h"
#include "mysqlproto.h"
#include "dispatcher.h"

enum conntype {
	LISTENER,
	CONNECTION
};

enum connstate {
	SENDHANDSHAKEV10,
	RECVHANDSHAKEV10,
	HANDSHAKEV10_RECEIVED,
	RECVHANDSHAKERESPV10,
	SENDHANDSHAKERESPV10,
	WAITUPSTREAMCONNS,
	LOGINOK,
	AFTERLOGIN,
	INPUT,
	READY,
	RESULT,
	RESULT_EOF,
	FAIL,
	QUERY,
	QUERY_SENT,
	QUERY_FORWARDED,
	QUERY_ERR,
	HANDLED,
	RESULTSET,
	QUIT
};

typedef struct _connection {
	int sock;
	char takenby;   /* -2: being setup, -1: free, 0: not taken, >0: tid */
	packetbuf *pkt;
	unsigned char needpkt:1;
	unsigned char sendallresults:1;
	connprops props;
	unsigned char seq;
	time_t wait;
	enum connstate state;
	char *sqlstate;
	char *errmsg;
	int upstreamslen;
	int *upstreams;
	unsigned char goteof:1;
	unsigned char resultsent:1;
	long long int resultcols;
	int results;
	int upstream;
} connection;

struct _dispatcher {
	pthread_t tid;
	enum conntype type;
	char id;
	size_t queries;
	size_t ticks;
	enum { RUNNING, SLEEPING } state;
	char keep_running:1;
};

static connection *listeners[32];       /* hopefully enough */
static connection *connections = NULL;
static size_t connectionslen = 0;
pthread_rwlock_t connectionslock = PTHREAD_RWLOCK_INITIALIZER;
static size_t acceptedconnections = 0;
static size_t closedconnections = 0;


/**
 * Adds an (initial) listener socket to the chain of connections.
 * Listener sockets are those which need to be accept()-ed on.
 */
int
dispatch_addlistener(int sock)
{
	connection *newconn;
	int c;

	newconn = malloc(sizeof(connection));
	if (newconn == NULL)
		return 1;
	(void) fcntl(sock, F_SETFL, O_NONBLOCK);
	newconn->sock = sock;
	newconn->takenby = 0;
	for (c = 0; c < sizeof(listeners) / sizeof(connection *); c++)
		if (__sync_bool_compare_and_swap(&(listeners[c]), NULL, newconn))
			break;
	if (c == sizeof(listeners) / sizeof(connection *)) {
		char nowbuf[24];
		free(newconn);
		fprintf(stderr, "[%s] cannot add new listener: "
				"no more free listener slots (max = %zd)\n",
				fmtnow(nowbuf), sizeof(listeners) / sizeof(connection *));
		return 1;
	}

	return 0;
}

void
dispatch_removelistener(int sock)
{
	int c;
	connection *conn;

	/* find connection */
	for (c = 0; c < sizeof(listeners) / sizeof(connection *); c++)
		if (listeners[c] != NULL && listeners[c]->sock == sock)
			break;
	if (c == sizeof(listeners) / sizeof(connection *)) {
		/* not found?!? */
		fprintf(stderr, "dispatch: cannot find listener!\n");
		return;
	}
	/* make this connection no longer visible */
	conn = listeners[c];
	listeners[c] = NULL;
	/* if some other thread was looking at conn, make sure it
	 * will have moved on before freeing this object */
	usleep(10 * 1000);  /* 10ms */
	close(conn->sock);
	free(conn);
}

#define CONNGROWSZ  1024

/**
 * Adds a connection socket to the chain of connections.
 * Connection sockets are those which need to be read from.
 */
static int
dispatch_addconnection(int sock, enum connstate istate)
{
	size_t c;

	pthread_rwlock_rdlock(&connectionslock);
	for (c = 0; c < connectionslen; c++)
		if (__sync_bool_compare_and_swap(&(connections[c].takenby), -1, -2))
			break;
	pthread_rwlock_unlock(&connectionslock);

	if (c == connectionslen) {
		char nowbuf[24];
		connection *newlst;

		pthread_rwlock_wrlock(&connectionslock);
		if (connectionslen > c) {
			/* another dispatcher just extended the list */
			pthread_rwlock_unlock(&connectionslock);
			return dispatch_addconnection(sock, istate);
		}
		newlst = realloc(connections,
				sizeof(connection) * (connectionslen + CONNGROWSZ));
		if (newlst == NULL) {
			fprintf(stderr, "[%s] cannot add new connection: "
					"out of memory allocating more slots (max = %zd)\n",
					fmtnow(nowbuf), connectionslen);

			pthread_rwlock_unlock(&connectionslock);
			return -1;
		}

		memset(&newlst[connectionslen], '\0',
				sizeof(connection) * CONNGROWSZ);
		for (c = connectionslen; c < connectionslen + CONNGROWSZ; c++)
			newlst[c].takenby = -1;  /* free */
		connections = newlst;
		c = connectionslen;  /* for the setup code below */
		newlst[c].takenby = -2;
		connectionslen += CONNGROWSZ;

		pthread_rwlock_unlock(&connectionslock);
	}

	(void) fcntl(sock, F_SETFL, O_NONBLOCK);
	connections[c].sock = sock;
	connections[c].pkt = NULL;
	memset(&connections[c].props, 0, sizeof(connprops));
	connections[c].seq = 0;
	connections[c].wait = 0;
	connections[c].state = istate;
	connections[c].upstreamslen = 0;
	connections[c].upstreams = NULL;
	connections[c].needpkt = 0;
	connections[c].sendallresults = 1;  /* merge results by default */
	connections[c].takenby = 0;  /* now dispatchers will pick this one up */
	acceptedconnections++;

	return c;
}

static void
handle_packet(dispatcher *self, connection *conn)
{
	switch (conn->state) {
		case RECVHANDSHAKEV10:
			if (recv_handshakev10(conn->pkt, &conn->props) == NULL) {
				fprintf(stderr, "failed to parse package from server\n");
			} else {
				conn->state = HANDSHAKEV10_RECEIVED;
			}
			break;
		case RECVHANDSHAKERESPV10:
			recv_handshakeresponsev41(conn->pkt, &conn->props);
			conn->seq++;
			conn->state = WAITUPSTREAMCONNS;
			break;
		case AFTERLOGIN:
			switch (conn->pkt->buf[0]) {
				case MYSQL_OK:
					conn->state = READY;
#ifdef DEBUG
					printf("fd %d: connected to server [%d/%s]\n",
							conn->sock, conn->props.connid,
							conn->props.sver);
#endif
					break;
				case MYSQL_ERR: {
					char *err;
					err = recv_err(conn->pkt, conn->props.capabilities);
					fprintf(stderr, "fd %d: [%d/%s] failed to login: %s\n",
							conn->sock, conn->props.connid,
							conn->props.sver, err);
					free(err);
					conn->state = FAIL;
				}	break;
				case MYSQL_EOF:
					fprintf(stderr, "fd %d: authswithrequest: %s, %s\n",
							conn->sock,
							&conn->pkt->buf[1],
							&conn->pkt->buf[1 + strlen((char *)&conn->pkt->buf[1]) + 1]);
					conn->state = FAIL;
					break;
				default:
					fprintf(stderr, "fd %d: unhandled response after "
							"login: %X\n", conn->sock, conn->pkt->buf[0]);
					conn->state = FAIL;
					break;
			}
			break;
		case INPUT:
			self->queries++;
			conn->seq = packetbuf_hdr_seq(conn->pkt);

			switch (conn->pkt->buf[0]) {
				case COM_PING: {
					mysql_ok ok;

					ok.affrows = 0;
					ok.lastid = 0;
					ok.status_flags = SERVER_STATUS_AUTOCOMMIT;
					ok.warnings = 0;
					ok.status_info = "I'm still here!";
					ok.session_state_info = NULL;
					send_ok(conn->sock, ++conn->seq,
							conn->props.capabilities, &ok);
				} break;
				case COM_QUIT:
					conn->state = QUIT;
					break;
				case COM_STATISTICS: {
					char buf[512];
					time_t now;
					time(&now);
					snprintf(buf, sizeof(buf),
							"Uptime: %d  "
							"Accepted connections: %zd  "
							"Closed connections: %zd  "
							"Connected backends: %d"
							,
							(int)now - startuptime,
							acceptedconnections,
							closedconnections,
							conn->upstreamslen
							);
#ifdef DEBUG
					printf("COM_STATISTICS: %s\n", buf);
#endif
					send_eof_str(conn->sock, ++conn->seq, buf);
				}	break;
				case COM_QUERY: {
#ifdef DEBUG
					char *q = recv_comquery(conn->pkt);
					printf("COM_QUERY: %s\n", q);
					free(q);
#endif
					conn->needpkt = 1;
					conn->state = QUERY;
				}	break;
				case COM_INIT_DB: {
#ifdef DEBUG
					char *feature = recv_eof_str(conn->pkt);
					printf("COM_INIT_DB: %s\n", feature);
#endif
					if (conn->pkt->len > 9 &&
							strncmp((char *)&conn->pkt->buf[1], "feature:", 8) == 0)
					{
#ifndef DEBUG
						char *feature = recv_eof_str(conn->pkt);
#endif

						if (strcmp(feature + 8, "allresults") == 0) {
							/* enable returning all results */
							mysql_ok ok;

							conn->sendallresults = 1;
							ok.affrows = 0;
							ok.lastid = 0;
							ok.status_flags = SERVER_STATUS_AUTOCOMMIT;
							ok.warnings = 0;
							ok.status_info = "result merging enabled";
							ok.session_state_info = NULL;
							send_ok(conn->sock, ++conn->seq,
									conn->props.capabilities, &ok);
						} else if (strcmp(feature + 8, "firstresult") == 0) {
							/* only return first result (default) */
							mysql_ok ok;

							conn->sendallresults = 0;
							ok.affrows = 0;
							ok.lastid = 0;
							ok.status_flags = SERVER_STATUS_AUTOCOMMIT;
							ok.warnings = 0;
							ok.status_info = "result merging disabled";
							ok.session_state_info = NULL;
							send_ok(conn->sock, ++conn->seq,
									conn->props.capabilities, &ok);
						} else {
							send_err(conn->sock, ++conn->seq,
									conn->props.capabilities,
									"PQ004", "Unknown feature");
						}

						free(feature);
						break;
					}
#ifdef DEBUG
					free(feature);
#endif

					conn->needpkt = 1;
					conn->state = QUERY;
				}	break;
				default:
					send_err(conn->sock, ++conn->seq, conn->props.capabilities,
							"PQ001", "Unhandled command");
					break;
			}
			break;
		case QUERY_SENT:
			switch (conn->pkt->buf[0]) {
				case MYSQL_OK:
					conn->needpkt = 1;
					conn->state = READY;
					break;
				case MYSQL_ERR:
					conn->needpkt = 1;
					conn->state = FAIL;
					break;
				case MYSQL_EOF:
					/* the EOF packet may appear in places where a
					 * Protocol::LengthEncodedInteger may appear. You
					 * must check whether the packet length is less than
					 * 9 to make sure that it is a EOF packet */
					if (packetbuf_hdr_len(conn->pkt) < 9) {
						conn->needpkt = 1;
						conn->state = RESULT_EOF;
						break;
					}
				default:
					conn->needpkt = 1;
					conn->state = RESULT;
					break;
			}
			break;
		default:
			fprintf(stderr, "don't know how to handle packet in state %d\n",
					conn->state);
			break;
	}
}

/**
 * Look at conn and see if works needs to be done.  If so, do it.
 */
static int
dispatch_connection(connection *conn, dispatcher *self)
{
	int ret = 0;
	struct timeval start, stop;

	gettimeofday(&start, NULL);

	switch (conn->state) {
		case SENDHANDSHAKEV10:
			conn->props.sver = strdup("5.7-mysqlpq-" VERSION);
			conn->props.status = SERVER_STATUS_AUTOCOMMIT;
			conn->props.connid = (int)acceptedconnections;
			conn->props.charset = 0x21;  /* UTF-8 */
			conn->props.chal = strdup("12345678123456789012");
			conn->props.auth = strdup("mysql_native_password");
			conn->props.capabilities = CLIENT_BASIC_FLAGS;
			send_handshakev10(conn->sock, conn->seq, &conn->props);
			/* fork off connections to backends */
			{
				int fd;
				char nowbuf[24];
				int c;
				int i;
				struct addrinfo hints, *res, *res0;
				int error;
				const char *cause = NULL;

				conn->upstreamslen = 0;
				conn->upstreams = malloc(sizeof(int) * connect_host_cnt);

				for (i = 0; i < connect_host_cnt; i++) {
					memset(&hints, 0, sizeof(hints));
					hints.ai_family = PF_UNSPEC;
					hints.ai_socktype = SOCK_STREAM;
					if ((error = getaddrinfo(connect_hosts[i], "3306",
							&hints, &res0)) != 0)
					{
						fprintf(stderr, "[%s] failed to resolve %s: %s\n",
								fmtnow(nowbuf), connect_hosts[i],
								gai_strerror(error));
						continue;
					}

					fd = -1;
					for (res = res0; res; res = res->ai_next) {
						if ((fd = socket(res->ai_family, res->ai_socktype,
										res->ai_protocol)) < 0)
						{
							cause = "create";
							fprintf(stderr, "[%s] failed to create socket: %s\n",
									fmtnow(nowbuf), strerror(errno));
							continue;
						}


						if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
							cause = "connect";
							close(fd);
							fd = -1;
							continue;
						}

						c = dispatch_addconnection(fd, RECVHANDSHAKEV10);
						if (c >= 0)
							conn->upstreams[conn->upstreamslen++] = c;
					}
					if (fd < 0) {
						fprintf(stderr, "[%s] failed to %s socket for %s:3306: %s\n",
								fmtnow(nowbuf), cause,
								connect_hosts[i], strerror(errno));
						break;
					}
					freeaddrinfo(res0);
				}
			}
			conn->state = RECVHANDSHAKERESPV10;
			ret = 1;
			break;
		case WAITUPSTREAMCONNS: {
			int i;
			char ok = 1;
			
			for (i = 0; i >= 0 && i < conn->upstreamslen; i++) {
				connection *c = &connections[conn->upstreams[i]];
				switch (c->state) {
					case READY:
					case FAIL:
						break;
					case HANDSHAKEV10_RECEIVED:
						c->props.capabilities &= conn->props.capabilities;
						/* get rid of CLIENT_SESSION_TRACK because it
						 * means the server can send stuff after an OK
						 * packet */
						c->props.capabilities &= ~CLIENT_SESSION_TRACK;
						c->props.username = strdup(connect_username);
						c->props.passwd = strdup(connect_passwd);
						c->props.auth = strdup("mysql_native_password");
						c->props.dbname = conn->props.dbname == NULL ?
							NULL : strdup(conn->props.dbname);
						c->props.maxpktsize = conn->props.maxpktsize;
						c->state = SENDHANDSHAKERESPV10;
						ok = 0;
						break;
					default:
						ok = 0;
						/* wait for the connections to commence */
						break;
				}
			}
			if (ok == 1) {
				conn->state = LOGINOK;
				ret = 1;
			}
		}	break;
		case SENDHANDSHAKERESPV10:
			send_handshakeresponsev41(conn->sock, ++conn->seq, &conn->props);
			conn->state = AFTERLOGIN;
			ret = 1;
			break;
		case LOGINOK: {
			int i;
			int ready = 0;
			int total = conn->upstreamslen;
			char info[256];
			mysql_ok ok;

			for (i = 0; i < conn->upstreamslen; i++) {
				switch (connections[conn->upstreams[i]].state) {
					case READY:
						ready++;
						break;
					case FAIL:
						/* remove connections that aren't usable */
						connections[conn->upstreams[i]].state = QUIT;
						memmove(&conn->upstreams[i],
								&conn->upstreams[i + 1],
								--conn->upstreamslen - i);
						i--;  /* force evaluation of item put in place */
						break;
					default:
						break;
				}
			}
			snprintf(info, sizeof(info), "Logged in with %d out of %d "
					"upstream connections active\n",
					ready, total);
			ok.affrows = 0;
			ok.lastid = 0;
			ok.status_flags = SERVER_STATUS_AUTOCOMMIT;
			ok.warnings = 0;
			ok.status_info = info;
			ok.session_state_info = NULL;
			send_ok(conn->sock, ++conn->seq, conn->props.capabilities, &ok);
			conn->state = INPUT;
			ret = 1;
		}	break;
		case QUERY:
			if (conn->upstreams == NULL || conn->upstreamslen == 0) {
				packetbuf_free(conn->pkt);
				conn->pkt = NULL;
				conn->state = QUERY_ERR;
				conn->sqlstate = "PQ002";
				conn->errmsg = "No connected endpoints";
			} else {
				int i;

				/* conn->pkt is the query pkt from the client, so we can
				 * just send this to the upstream servers */
				for (i = 0; i < conn->upstreamslen; i++) {
					connection *c = &connections[conn->upstreams[i]];
					packetbuf_forward(conn->pkt, c->sock);
					c->state = QUERY_SENT;
				}
				packetbuf_free(conn->pkt);
				conn->pkt = NULL;
				conn->resultsent = 0;
				conn->state = QUERY_FORWARDED;
				ret = 1;
				break;
			} /* fall through for err in first branch of if-case */
		case QUERY_ERR:
			send_err(conn->sock, ++conn->seq, conn->props.capabilities,
					conn->sqlstate, conn->errmsg);
			conn->state = INPUT;
			ret = 1;
			break;
		case QUERY_FORWARDED: {
			int i;
			int ready = -1;
			int fail = -1;
			int result = -1;

			conn->results = 0;
			for (i = 0; i >= 0 && i < conn->upstreamslen; i++) {
				switch (connections[conn->upstreams[i]].state) {
					case READY:
						ready = i;
						break;
					case FAIL:
					case RESULT_EOF:
						fail = i;
						break;
					case RESULT:
						result = i;
						conn->results++;
						assert(conn->results <= conn->upstreamslen);
						break;
					case HANDLED:
						break;
					default:
						/* wait for the connections to commence */
						i = -2;
						break;
				}
			}

			if (i > -1) {
				/* send back working answer */
				if (result > -1) {
					connection *c = &connections[conn->upstreams[result]];
					assert(c->needpkt);
					if (!conn->resultsent) {
						conn->resultcols = recv_field_count(c->pkt);
						packetbuf_send(c->pkt, ++conn->seq, conn->sock);
					} else if (conn->sendallresults &&
							conn->resultcols != recv_field_count(c->pkt))
					{
						conn->sendallresults = 0;
						fprintf(stderr, "fd %d: disabling feature allresults: "
								"got two results with different column "
								"counts\n", conn->sock);
					}
					free(c->pkt);
					c->pkt = NULL;
					c->goteof = 0;
					c->resultcols = conn->resultcols;
					c->state = QUERY_SENT;
					conn->upstream = result;
					conn->state = RESULTSET;
				} else if (ready > -1) {
					connection *c = &connections[conn->upstreams[ready]];
					assert(c->needpkt);
					if (!conn->resultsent) {
						packetbuf_forward(c->pkt, conn->sock);
						conn->resultsent = 1;
					}
					free(c->pkt);
					c->pkt = NULL;
					c->state = HANDLED;
				} else if (fail > -1) {
					connection *c = &connections[conn->upstreams[fail]];
					assert(c->needpkt);
					if (!conn->resultsent) {
						packetbuf_forward(c->pkt, conn->sock);
						conn->resultsent = 1;
					}
					free(c->pkt);
					c->pkt = NULL;
					c->state = HANDLED;
				} else {
					/* all HANDLED */
					for (i = 0; i >= 0 && i < conn->upstreamslen; i++)
						connections[conn->upstreams[i]].state = READY;
					conn->state = INPUT;
				}

				ret = 1;
			}
		}	break;
		case RESULTSET: {
			char done = 0;
			connection *c = &connections[conn->upstreams[conn->upstream]];

			/* Since resultsets consist of many packets, we need to keep
			 * on forwarding packets, until we've sent everything.  This
			 * is not as easy as it sounds, as the protocol basically
			 * demands to look at the packets floating by in order to
			 * determine if the result is "complete".  Because there are
			 * two versions (using EOF or not) the logic consists of
			 * finding the second EOF or an OK/ERR. */
			switch (c->state) {
				case READY: {
					mysql_ok *ok = recv_ok(c->pkt, c->props.capabilities);
					/* we should only see OK if the client doesn't
					 * expect EOFs, so we can assume being done here */
					done = !(ok->status_flags & SERVER_MORE_RESULTS_EXISTS);

					if (ok->status_info)
						free(ok->status_info);
					if (ok->session_state_info)
						free(ok->session_state_info);
					free(ok);
				}	break;
				case FAIL:
					/* whether or not we look for EOF, if we see ERR
					 * it's the end for this sequence */
					done = 1;
					break;
				case RESULT_EOF:
					/* the case where we've set CLIENT_DEPRECATE_EOF
					 * seems to be violated in practise, so just accept
					 * EOF if we see it and act as if we're in EOF mode */
					if (c->goteof) {
						mysql_eof *eof =
							recv_eof(c->pkt, c->props.capabilities);

						/* last EOF, so end of this thing, unless
						 * status_flags indicates more is to come */
						done = !(eof->status_flags &
								SERVER_MORE_RESULTS_EXISTS);

						free(eof);
					} else {
						/* delay this for below, we need to skip the EOF
						 * when using feature:allresults
						c->goteof = 1;
						 */
					}
					break;
				case RESULT:
					/* calculate when the column names have been seen */
					if (c->resultcols == 0)
						c->goteof = 1;
					if (!c->goteof)
						c->resultcols--;
					break;
				default:
					/* wait for the next answer */
					done = -1;
					break;
			}
			if (done >= 0) {
#ifdef DEBUG
				printf("done: %d, cols: %lld, goteof: %d, results: %d\n",
						done, c->resultcols, c->goteof, conn->results);
#endif

				/*
				 * cols
				 * fields
				 *  eof      conn->goteof == 1
				 * rows
				 *  eof|ok   done == 1
				 */
				if (conn->sendallresults) {
					if ((!done && (!conn->resultsent ||
								(conn->resultsent && c->goteof))) ||
							(done && conn->results == 1))
						packetbuf_send(c->pkt, ++conn->seq, conn->sock);
				} else if (!conn->resultsent) {
					packetbuf_send(c->pkt, ++conn->seq, conn->sock);
				}

				packetbuf_free(c->pkt);
				c->pkt = NULL;
				/* delayed set, see above */
				if (c->state == RESULT_EOF)
					c->goteof = 1;
				if (done) {
					c->state = HANDLED;
					conn->resultsent = 1;
					conn->state = QUERY_FORWARDED;
				} else {
					c->state = QUERY_SENT;
				}

				ret = 1;
			}
		}	break;
		case QUIT:
			/* take the easy way: just close the connection */
			closedconnections++;
			close(conn->sock);

#ifdef DEBUG
			printf("fd %d: closing\n", conn->sock);
#endif
			/* close upstream connections also */
			while (conn->upstreamslen > 0) {
				conn->upstreamslen--;
				connections[conn->upstreams[conn->upstreamslen]].state = QUIT;
			}

			/* flag this connection as no longer in use */
			conn->takenby = -1;

			gettimeofday(&stop, NULL);
			self->ticks += timediff(start, stop);
			return 0;  /* unlikely new stuff has to be done for this conn */
		case READY:
		case RESULT:
		case RESULT_EOF:
		case HANDLED:
		case FAIL:
			break;
		default: {
			int len;
			if ((len = packetbuf_recv_data(&conn->pkt, conn->sock)) > 0) {
				/* packet done, deal with it */
				conn->needpkt = 0;
				handle_packet(self, conn);
				if (!conn->needpkt) {
					packetbuf_free(conn->pkt);
					conn->pkt = NULL;
				}
				ret = 1;
			} else if (len == -1) {
				if (conn->pkt == NULL)
					fprintf(stderr, "out of memory allocating packet buffer!\n");
#ifdef DEBUG
				fprintf(stderr, "fd %d: failed to read(%d): EOF or error: %s\n",
						conn->sock, len, strerror(errno));
#endif
				conn->state = QUIT;
				ret = 1;
			} else {
				ret = len != -2;
			}
		}	break;
	}

	gettimeofday(&stop, NULL);
	self->ticks += timediff(start, stop);

	/* "release" this connection again */
	conn->takenby = 0;

	return ret;
}

/**
 * pthread compatible routine that handles connections and processes
 * whatever comes in on those.
 */
static void *
dispatch_runner(void *arg)
{
	dispatcher *self = (dispatcher *)arg;
	connection *conn;
	int work;
	int c;

	self->queries = 0;
	self->ticks = 0;
	self->state = SLEEPING;

	if (self->type == LISTENER) {
		fd_set fds;
		int maxfd = -1;
		struct timeval tv;
		while (self->keep_running) {
			FD_ZERO(&fds);
			tv.tv_sec = 0;
			tv.tv_usec = 250 * 1000;  /* 250 ms */
			for (c = 0; c < sizeof(listeners) / sizeof(connection *); c++) {
				conn = listeners[c];
				if (conn == NULL)
					break;
				FD_SET(conn->sock, &fds);
				if (conn->sock > maxfd)
					maxfd = conn->sock;
			}
			if (select(maxfd + 1, &fds, NULL, NULL, &tv) > 0) {
				for (c = 0; c < sizeof(listeners) / sizeof(connection *); c++) {
					conn = listeners[c];
					if (conn == NULL)
						break;
					if (FD_ISSET(conn->sock, &fds)) {
						int client;
						struct sockaddr addr;
						socklen_t addrlen = sizeof(addr);

						if ((client = accept(conn->sock, &addr, &addrlen)) < 0)
						{
							char nowbuf[24];
							fprintf(stderr, "[%s] dispatch: failed to "
									"accept() new connection: %s\n",
									fmtnow(nowbuf), strerror(errno));
							continue;
						}
						if (dispatch_addconnection(client, SENDHANDSHAKEV10) < 0) {
							close(client);
							continue;
						}
					}
				}
			}
		}
	} else if (self->type == CONNECTION) {
		while (self->keep_running) {
			work = 0;
			pthread_rwlock_rdlock(&connectionslock);
			for (c = 0; c < connectionslen; c++) {
				conn = &(connections[c]);
				/* atomically try to "claim" this connection */
				if (!__sync_bool_compare_and_swap(&(conn->takenby), 0, self->id))
					continue;
				self->state = RUNNING;
				work += dispatch_connection(conn, self);
			}
			pthread_rwlock_unlock(&connectionslock);

			self->state = SLEEPING;
			/* nothing done, avoid spinlocking */
			if (self->keep_running && work == 0)
				usleep((100 + (rand() % 200)) * 1000);  /* 100ms - 300ms */
		}
	} else {
		fprintf(stderr, "huh? unknown self type!\n");
	}

	return NULL;
}

/**
 * Starts a new dispatcher for the given type and with the given id.
 * Returns its handle.
 */
static dispatcher *
dispatch_new(char id, enum conntype type)
{
	dispatcher *ret = malloc(sizeof(dispatcher));

	if (ret == NULL)
		return NULL;

	ret->id = id;
	ret->type = type;
	ret->keep_running = 1;
	if (pthread_create(&ret->tid, NULL, dispatch_runner, ret) != 0) {
		free(ret);
		return NULL;
	}

	return ret;
}

static char globalid = 0;

/**
 * Starts a new dispatcher specialised in handling incoming connections
 * (and putting them on the queue for handling the connections).
 */
dispatcher *
dispatch_new_listener(void)
{
	char id = __sync_fetch_and_add(&globalid, 1);
	return dispatch_new(id, LISTENER);
}

/**
 * Starts a new dispatcher specialised in handling incoming data on
 * existing connections.
 */
dispatcher *
dispatch_new_connection(void)
{
	char id = __sync_fetch_and_add(&globalid, 1);
	return dispatch_new(id, CONNECTION);
}

/**
 * Signals this dispatcher to stop whatever it's doing.
 */
void
dispatch_stop(dispatcher *d)
{
	d->keep_running = 0;
}

/**
 * Shuts down and frees up dispatcher d.  Returns when the dispatcher
 * has terminated.
 */
void
dispatch_shutdown(dispatcher *d)
{
	dispatch_stop(d);
	pthread_join(d->tid, NULL);
	free(d);
}

/**
 * Returns the wall-clock time in milliseconds consumed by this dispatcher.
 */
inline size_t
dispatch_get_ticks(dispatcher *self)
{
	return self->ticks;
}

/**
 * Returns the number of queries dispatched since start.
 */
inline size_t
dispatch_get_queries(dispatcher *self)
{
	return self->queries;
}

/**
 * Returns whether this dispatcher is currently running, or not.  A
 * dispatcher is running when it is actively handling a connection, and
 * all tasks related to getting the data received in the place where it
 * should be.
 */
inline char
dispatch_busy(dispatcher *self)
{
	return self->state == RUNNING;
}

/**
 * Returns the number of accepted connections thusfar.
 */
size_t
dispatch_get_accepted_connections(void)
{
	return acceptedconnections;
}

/**
 * Returns the number of closed connections thusfar.
 */
size_t
dispatch_get_closed_connections(void)
{
	return closedconnections;
}
