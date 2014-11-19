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

#include "mysqlpq.h"
#include "collector.h"
#include "mysqlproto.h"

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
	RESULTSET,
	QUIT
};

typedef struct _connection {
	int sock;
	char takenby;   /* -2: being setup, -1: free, 0: not taken, >0: tid */
	packetbuf *pkt;
	char needpkt:1;
	connprops props;
	char seq;
	time_t wait;
	enum connstate state;
	char *sqlstate;
	char *errmsg;
	int upstreamslen;
	int *upstreams;
	char goteof:1;
	int upstream;
} connection;

typedef struct _dispatcher {
	pthread_t tid;
	enum conntype type;
	char id;
	size_t metrics;
	size_t ticks;
	enum { RUNNING, SLEEPING } state;
	char keep_running:1;
} dispatcher;

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
	connections[c].takenby = 0;  /* now dispatchers will pick this one up */
	acceptedconnections++;

	return c;
}

#if 0
static inline int
read_input(connection *conn)
{
	int len = -2;
	/* try to read more data, if that succeeds, or we still have data
	 * left in the buffer, try to process the buffer */
	if (
			(!conn->needmore && conn->buflen > 0) || 
			(len = read(conn->sock,
						conn->buf + conn->buflen, 
						(sizeof(conn->buf) - 1) - conn->buflen)) > 0
	   )
	{
		if (len > 0)
			conn->buflen += len;

		/* TODO: do something */
	}
	if (len == -1 && (errno == EINTR ||
				errno == EAGAIN ||
				errno == EWOULDBLOCK))
	{
		/* nothing available/no work done */
		if (conn->wait == 0) {
			conn->wait = time(NULL);
			conn->takenby = 0;
			return 0;
		} else if (time(NULL) - conn->wait > IDLE_DISCONNECT_TIME) {
			/* force close connection below */
			len = 0;
		} else {
			conn->takenby = 0;
			return 0;
		}
	}
	if (len == -1 || len == 0) {  /* error + EOF */
		/* we also disconnect the client in this case if our reading
		 * buffer is full, but we still need more (read returns 0 if the
		 * size argument is 0) -> this is good, because we can't do much
		 * with such client */

		closedconnections++;
		close(conn->sock);

		/* flag this connection as no longer in use */
		conn->takenby = -1;

		return 0;
	}

	/* "release" this connection again */
	conn->takenby = 0;

	return 1;
}
#endif

static void
handle_packet(connection *conn)
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
			conn->state = WAITUPSTREAMCONNS;
			break;
		case AFTERLOGIN:
			switch (conn->pkt->buf[0]) {
				case MYSQL_OK:
					conn->state = READY;
					break;
				case MYSQL_ERR: {
					char *err;
					conn->state = FAIL;
					err = recv_err(conn->pkt, conn->props.capabilities);
					fprintf(stderr, "failed to login: %s\n", err);
					free(err);
				}	break;
				case 0xfe:
					fprintf(stderr, "authswithrequest: %s, %s\n",
							&conn->pkt->buf[1],
							&conn->pkt->buf[1 + strlen((char *)&conn->pkt->buf[1]) + 1]);
					conn->state = FAIL;
					break;
				default:
					fprintf(stderr, "unhandled response after login: %X\n",
							conn->pkt->buf[0]);
					conn->state = FAIL;
					break;
			}
			break;
		case INPUT:
			switch (conn->pkt->buf[0]) {
				case COM_QUIT:
					conn->state = QUIT;
					break;
				case COM_QUERY: {
					conn->needpkt = 1;
					conn->state = QUERY;
				}	break;
				case COM_INIT_DB:
					conn->needpkt = 1;
					conn->state = QUERY;
					break;
				default:
					send_err(conn->sock, conn->seq, conn->props.capabilities,
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

	conn->seq = packetbuf_hdr_seq(conn->pkt) + 1;
}

#define IDLE_DISCONNECT_TIME  (10 * 60)  /* 10 minutes */
/**
 * Look at conn and see if works needs to be done.  If so, do it.
 */
static int
dispatch_connection(connection *conn, dispatcher *self)
{
	int ret = 1;
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
				struct sockaddr_in serv_addr;
				char nowbuf[24];
				int c;

				conn->upstreamslen = 0;
				conn->upstreams = malloc(sizeof(int) * 1); /* FIXME 1 server */

				if ((fd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
					fprintf(stderr, "[%s] failed to create socket: %s\n",
							fmtnow(nowbuf), strerror(errno));
					break;
				}

				serv_addr.sin_family = PF_INET;
				serv_addr.sin_port = htons(3306);
				inet_pton(PF_INET, "localhost", &serv_addr.sin_addr);

				if (connect(fd,
							(struct sockaddr *)&serv_addr, sizeof(serv_addr)))
				{
					fprintf(stderr, "[%s] failed to connect socket: %s\n",
							fmtnow(nowbuf), strerror(errno));
					break;
				}

				c = dispatch_addconnection(fd, RECVHANDSHAKEV10);
				if (c >= 0)
					conn->upstreams[conn->upstreamslen++] = c;
			}
			conn->state = RECVHANDSHAKERESPV10;
			break;
		case WAITUPSTREAMCONNS: {
			int i;
			
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
						c->props.username = strdup("test");
						c->props.passwd = strdup("test");
						c->props.auth = strdup("mysql_native_password");
						c->props.dbname = conn->props.dbname == NULL ?
							NULL : strdup(conn->props.dbname);
						c->props.maxpktsize = conn->props.maxpktsize;
						c->state = SENDHANDSHAKERESPV10;
						break;
					default:
						/* wait for the connections to commence */
						i = -1;
						break;
				}
			}
			if (i > -1)
				conn->state = LOGINOK;
		}	break;
		case SENDHANDSHAKERESPV10:
			send_handshakeresponsev41(conn->sock, conn->seq, &conn->props);
			conn->state = AFTERLOGIN;
			break;
		case LOGINOK: {
			int i;
			int ready = 0;
			int total = conn->upstreamslen;
			char info[256];
			for (i = 0; i < conn->upstreamslen; i++) {
				switch (connections[conn->upstreams[i]].state) {
					case READY:
						ready++;
						break;
					case FAIL:
						/* remove connections that aren't usable */
						close(connections[conn->upstreams[i]].sock);
						memmove(&conn->upstreams[i],
								&conn->upstreams[i + 1],
								--conn->upstreamslen - i);
					default:
						break;
				}
			}
			snprintf(info, sizeof(info), "Logged in with %d out of %d "
					"upstream connections active\n",
					ready, total);
			send_ok(conn->sock, conn->seq, conn->props.capabilities, info);
			conn->state = INPUT;
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
				conn->state = QUERY_FORWARDED;
				break;
			} /* fall through for err in first branch of if-case */
		case QUERY_ERR:
			send_err(conn->sock, conn->seq, conn->props.capabilities,
					conn->sqlstate, conn->errmsg);
			conn->state = INPUT;
			break;
		case QUERY_FORWARDED: {
			int i;
			int ready = -1;
			int fail = -1;
			int result = -1;

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
						break;
					default:
						/* wait for the connections to commence */
						i = -1;
						break;
				}
			}

			if (i > -1) {
				/* send back working answer */
				if (result > -1) {
					connection *c = &connections[conn->upstreams[result]];
					packetbuf_forward(c->pkt, conn->sock);
					conn->upstream = result;
					conn->goteof = 0;
					conn->state = RESULTSET;
				} else if (ready > -1) {
					connection *c = &connections[conn->upstreams[ready]];
					packetbuf_forward(c->pkt, conn->sock);
					conn->state = INPUT;
				} else if (fail > -1) {
					connection *c = &connections[conn->upstreams[fail]];
					packetbuf_forward(c->pkt, conn->sock);
					conn->state = INPUT;
				} else {
					conn->state = QUERY_ERR;
					conn->sqlstate = "PQ003";
					conn->errmsg = "No answers from upstream servers ?!?";
					break;
				}

				for (i = 0; i >= 0 && i < conn->upstreamslen; i++) {
					connection *c = &connections[conn->upstreams[i]];
					packetbuf_free(c->pkt);
					c->pkt = NULL;
					c->state = i != result ? READY : QUERY_SENT;
				}
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
					if (c->props.capabilities & CLIENT_DEPRECATE_EOF) {
						/* barf, we don't expect this, let the client
						 * deal with it */
						done = 1;
					} else if (conn->goteof) {
						mysql_eof *eof =
							recv_eof(c->pkt, c->props.capabilities);

						/* last EOF, so end of this thing, unless
						 * status_flags indicates more is to come */
						done = !(eof->status_flags &
								SERVER_MORE_RESULTS_EXISTS);
						free(eof);
					} else {
						conn->goteof = 1;
					}
					break;
				case RESULT:
					break;
				default:
					/* wait for the next answer */
					done = -1;
					break;
			}
			if (done >= 0) {
				packetbuf_forward(c->pkt, conn->sock);
				packetbuf_free(c->pkt);
				c->pkt = NULL;
				if (done) {
					c->state = READY;
					conn->state = INPUT;
				} else {
					c->state = QUERY_SENT;
				}
			}
		}	break;
		case QUIT:
			/* take the easy way: just close the connection */
			closedconnections++;
			close(conn->sock);

			/* flag this connection as no longer in use */
			conn->takenby = -1;

			gettimeofday(&stop, NULL);
			self->ticks += timediff(start, stop);
			return 0;
		case READY:
		case RESULT:
		case RESULT_EOF:
		case FAIL:
			break;
		default:
			if (conn->pkt == NULL)
				conn->pkt = packetbuf_recv_hdr(conn->sock);
			if (conn->pkt == NULL) {
				ret = 0;
				break;
			}
			/* FIXME handle errors */
			if (packetbuf_recv_data(conn->pkt, conn->sock) > 0) {
				/* packet done, deal with it */
				conn->needpkt = 0;
				handle_packet(conn);
				if (!conn->needpkt) {
					packetbuf_free(conn->pkt);
					conn->pkt = NULL;
				}
			} else {
				printf("got insufficient data\n");
			}
			break;
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

	self->metrics = 0;
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
 * Returns the number of metrics dispatched since start.
 */
inline size_t
dispatch_get_metrics(dispatcher *self)
{
	return self->metrics;
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
