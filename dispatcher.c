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

#include "mysqlpq.h"
#include "collector.h"
#include "mysqlproto.h"

enum conntype {
	LISTENER,
	CONNECTION
};

enum connstate {
	INIT,
	HANDSHAKEV10,
	LOGINOK,
	INPUT,
	QUERY_ERR,
	QUIT
};

typedef struct _connection {
	int sock;
	char takenby;   /* -2: being setup, -1: free, 0: not taken, >0: tid */
	packetbuf *pkt;
	connprops props;
	int capabilities;
	char seq;
	char needmore:1;
	time_t wait;
	enum connstate state;
	char *sqlstate;
	char *errmsg;
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
int
dispatch_addconnection(int sock)
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
			return dispatch_addconnection(sock);
		}
		newlst = realloc(connections,
				sizeof(connection) * (connectionslen + CONNGROWSZ));
		if (newlst == NULL) {
			fprintf(stderr, "[%s] cannot add new connection: "
					"out of memory allocating more slots (max = %zd)\n",
					fmtnow(nowbuf), connectionslen);

			pthread_rwlock_unlock(&connectionslock);
			return 1;
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
	connections[c].capabilities = 0;
	connections[c].seq = 0;
	connections[c].needmore = 0;
	connections[c].wait = 0;
	connections[c].state = INIT;
	connections[c].takenby = 0;  /* now dispatchers will pick this one up */
	acceptedconnections++;

	return 0;
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
		case HANDSHAKEV10:
			conn->capabilities = recv_handshakeresponsev41(conn->pkt,
					&conn->props);
			conn->state = LOGINOK;
			break;
		case INPUT:
			switch (conn->pkt->buf[0]) {
				case COM_QUIT:
					conn->state = QUIT;
					break;
				case COM_QUERY: {
					char *q = recv_comquery(conn->pkt);
					if (q != NULL)
						free(q);
					conn->state = QUERY_ERR;
					conn->sqlstate = "PQ002";
					conn->errmsg = "No connected endpoints";
				}	break;
				default:
					send_err(conn->sock, conn->seq, conn->capabilities,
							"PQ001", "Unhandled command");
					break;
			}
			break;
		default:
			fprintf(stderr, "don't know how to handle packet\n");
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
		case INIT:
			conn->props.sver = strdup("5.7-mysqlpq-" VERSION);
			conn->props.status = 0x0002;  /* auto_commit */
			conn->props.connid = (int)acceptedconnections;
			conn->props.charset = 0x21;  /* UTF-8 */
			conn->props.chal = strdup("12345678123456789012");
			conn->props.auth = strdup("mysql_native_password");
			conn->props.capabilities = CLIENT_BASIC_FLAGS;
			send_handshakev10(conn->sock, conn->seq, &conn->props);
			conn->state = HANDSHAKEV10;
			break;
		case LOGINOK:
			send_ok(conn->sock, conn->seq, conn->capabilities);
			conn->state = INPUT;
			break;
		case QUERY_ERR:
			send_err(conn->sock, conn->seq, conn->capabilities,
					conn->sqlstate, conn->errmsg);
			conn->state = INPUT;
			break;
		case QUIT:
			/* take the easy way: just close the connection */
			closedconnections++;
			close(conn->sock);

			/* flag this connection as no longer in use */
			conn->takenby = -1;

			gettimeofday(&stop, NULL);
			self->ticks += timediff(start, stop);
			return 0;
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
				handle_packet(conn);
				packetbuf_free(conn->pkt);
				conn->pkt = NULL;
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
						if (dispatch_addconnection(client) != 0) {
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
