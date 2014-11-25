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
#include <errno.h>
#include <sys/socket.h>
#include <openssl/sha.h>

#include "mysqlpq.h"
#include "mysqlproto.h"

/* lame implementation of just about what we need */

#if DEBUG
#define PRINT_CAPABILITY(var, NAME)\
	if (var & NAME) \
		printf(" " #NAME);
#define PRINT_ALL_CAPABILITIES(var) \
	PRINT_CAPABILITY(var, CLIENT_LONG_PASSWORD); \
	PRINT_CAPABILITY(var, CLIENT_FOUND_ROWS); \
	PRINT_CAPABILITY(var, CLIENT_LONG_FLAG); \
	PRINT_CAPABILITY(var, CLIENT_CONNECT_WITH_DB); \
	PRINT_CAPABILITY(var, CLIENT_NO_SCHEMA); \
	PRINT_CAPABILITY(var, CLIENT_COMPRESS); \
	PRINT_CAPABILITY(var, CLIENT_ODBC); \
	PRINT_CAPABILITY(var, CLIENT_LOCAL_FILES); \
	PRINT_CAPABILITY(var, CLIENT_IGNORE_SPACE); \
	PRINT_CAPABILITY(var, CLIENT_PROTOCOL_41); \
	PRINT_CAPABILITY(var, CLIENT_INTERACTIVE); \
	PRINT_CAPABILITY(var, CLIENT_SSL); \
	PRINT_CAPABILITY(var, CLIENT_IGNORE_SIGPIPE); \
	PRINT_CAPABILITY(var, CLIENT_TRANSACTIONS); \
	PRINT_CAPABILITY(var, CLIENT_RESERVED); \
	PRINT_CAPABILITY(var, CLIENT_SECURE_CONNECTION); \
	PRINT_CAPABILITY(var, CLIENT_MULTI_STATEMENTS); \
	PRINT_CAPABILITY(var, CLIENT_MULTI_RESULTS); \
	PRINT_CAPABILITY(var, CLIENT_PS_MULTI_RESULTS); \
	PRINT_CAPABILITY(var, CLIENT_PLUGIN_AUTH); \
	PRINT_CAPABILITY(var, CLIENT_CONNECT_ATTRS); \
	PRINT_CAPABILITY(var, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA); \
	PRINT_CAPABILITY(var, CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS); \
	PRINT_CAPABILITY(var, CLIENT_SESSION_TRACK); \
	PRINT_CAPABILITY(var, CLIENT_DEPRECATE_EOF); \
	PRINT_CAPABILITY(var, CLIENT_SSL_VERIFY_SERVER_CERT); \
	PRINT_CAPABILITY(var, CLIENT_REMEMBER_OPTIONS);
#endif

typedef struct {
	unsigned char len[3];
	unsigned char seq;
} mysql_packet_hdr;


static packetbuf *
packetbuf_new(void)
{
	packetbuf *ret = malloc(sizeof(packetbuf));

	if (ret != NULL) {
		ret->buf = ret->pos = NULL;
		ret->size = ret->len = 0;
	}

	return ret;
}

#if 0
static packetbuf *
packetbuf_reset(packetbuf *buf)
{
	buf->pos = buf->buf;
	buf->len = 0;

	return buf;
}
#endif

static inline packetbuf *
packetbuf_realloc(packetbuf *buf, size_t len)
{
	/* ensure len will fit in buf */
	if (buf->size - buf->len < len) {
		size_t newsize = sizeof(mysql_packet_hdr) + buf->size + len;
		size_t oldpos = buf->pos - buf->buf;
		unsigned char *newbuf = realloc(
				buf->buf == NULL ?
					buf->buf :
					buf->buf - sizeof(mysql_packet_hdr),
				/* allocate on 4K block boundaries */
				newsize = ((newsize / 4096) + 1) * 4096);
		if (newbuf == NULL) {
			/* out of memory, erm? */
			fprintf(stderr, "packetbuf_push: out of memory allocating "
					"%zd bytes\n", newsize);
			return NULL;
		}
		buf->buf = newbuf + sizeof(mysql_packet_hdr);
		buf->pos = buf->buf + oldpos;
		buf->size = newsize - sizeof(mysql_packet_hdr);
	}

	return buf;
}

void
packetbuf_free(packetbuf *buf)
{
	if (buf->buf)
		free(buf->buf - sizeof(mysql_packet_hdr));
	buf->buf = buf->pos = NULL;
	free(buf);
}

static packetbuf *
packetbuf_get(void)
{
	/* intended to use a pool, so we reuse packets, for now, just alloc
	 * new stuff */
	packetbuf *ret = packetbuf_new();

	if (ret == NULL)
		return NULL;

	/* make sure enough space is allocated to actually store the header,
	 * which we need to read */
	return packetbuf_realloc(ret, 4);
}

inline int
packetbuf_hdr_len(packetbuf *buf)
{
	mysql_packet_hdr *hdr = (void *)(buf->buf - sizeof(mysql_packet_hdr));
	return hdr->len[0] + (hdr->len[1] << 8) + (hdr->len[2] << 16);
}

inline char
packetbuf_hdr_seq(packetbuf *buf)
{
	mysql_packet_hdr *hdr = (void *)(buf->buf - sizeof(mysql_packet_hdr));
	return hdr->seq;
}

int
packetbuf_send(packetbuf *buf, char seq, int fd)
{
	/* fill in the reserved bytes before buf */
	mysql_packet_hdr *hdr = (void *)(buf->buf - sizeof(mysql_packet_hdr));

	/* we only support single packets, but we could loop here and
	 * increment seq, yada yada */
	hdr->len[0] = buf->len & 0xFF;
	hdr->len[1] = (buf->len >> 8) & 0xFF;
	hdr->len[2] = (buf->len >> 16) & 0xFF;
	hdr->seq = seq;

#ifdef DEBUG
	printf("sending pkt, len: %zd, seq: %d\n", buf->len, seq);
#endif
	return write(fd, hdr, sizeof(mysql_packet_hdr) + buf->len);
}

int
packetbuf_forward(packetbuf *buf, int fd)
{
	mysql_packet_hdr *hdr = (void *)(buf->buf - sizeof(mysql_packet_hdr));
	int len = hdr->len[0] + (hdr->len[1] << 8) + (hdr->len[2] << 16);

#ifdef DEBUG
	printf("forwarding pkt, len: %d, seq: %d\n", len, hdr->seq);
#endif

	return write(fd, hdr, sizeof(mysql_packet_hdr) + len);
}

packetbuf *
packetbuf_recv_hdr(int fd)
{
	packetbuf *ret = packetbuf_get();
	mysql_packet_hdr *hdr = (void *)(ret->buf - sizeof(mysql_packet_hdr));

	if (read(fd, hdr, sizeof(mysql_packet_hdr)) != sizeof(mysql_packet_hdr)) {
		packetbuf_free(ret);
		return NULL;
	}

	return ret;
}

/* call this function as long as it returns 0, if -1 an error occurred,
 * else its the size of the completed packet */
int
packetbuf_recv_data(packetbuf *buf, int fd)
{
	int wantlen = packetbuf_hdr_len(buf);
	ssize_t readlen;

	packetbuf_realloc(buf, wantlen);

	wantlen -= buf->len;
	readlen = read(fd, buf->buf + buf->len, wantlen);

	if (readlen == wantlen) {
		return buf->len += readlen;
	} else if ((readlen == -1 &&
			(errno == EINTR ||
			 errno == EAGAIN ||
			 errno == EWOULDBLOCK)) ||
			readlen < wantlen)
	{
		/* partial data */
		if (readlen > 0)
			buf->len += readlen;
		return 0;
	} else {
		/* unexpected EOF or error */
		return -1;
	}
}

static packetbuf *
packetbuf_push(packetbuf *buf, void *data, size_t len)
{
	packetbuf_realloc(buf, len);

	memcpy(buf->pos, data, len);
#ifdef DEBUG
	{
		int i;
		printf("packetbuf_push(%zd):", len);
		for (i = 0; i < len; i++) {
			printf(" %02x", buf->pos[i]);
		}
		printf("\n");
	}
#endif
	buf->pos += len;
	buf->len += len;

	return buf;
}

static packetbuf *
packetbuf_shift(packetbuf *buf, void *ptr, size_t len)
{
	size_t avail = buf->len - (buf->pos - buf->buf);
	if (avail == 0 || len > avail) {
#ifdef DEBUG
		printf("packetbuf_shift(%zd): insufficient data (%zd)\n",
				len, buf->len - (buf->pos - buf->buf));
#endif
		return NULL;  /* FIXME: return code? */
	}

	memcpy(ptr, buf->pos, len);
#ifdef DEBUG
	{
		int i;
		printf("packetbuf_shift(%zd):", len);
		for (i = 0; i < len; i++) {
			printf(" %02x", buf->pos[i]);
		}
		printf("\n");
	}
#endif
	buf->pos += len;

	return buf;
}


/* all LSB */
static packetbuf *
push_int1(packetbuf *buf, char val)
{
	return packetbuf_push(buf, &val, 1);
}

static packetbuf *
push_int2(packetbuf *buf, short val)
{
	char vb[2];

	vb[0] = val & 0xFF;
	vb[1] = ((unsigned short)val >> 8) & 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

static packetbuf *
push_int3(packetbuf *buf, int val)
{
	char vb[3];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) & 0xFF;
	vb[2] = (val >> 16) & 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

static packetbuf *
push_int4(packetbuf *buf, int val)
{
	char vb[4];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) & 0xFF;
	vb[2] = (val >> 16) & 0xFF;
	vb[3] = ((unsigned int)val >> 24) & 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

static packetbuf *
push_int6(packetbuf *buf, long long int val)
{
	char vb[6];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) & 0xFF;
	vb[2] = (val >> 16) & 0xFF;
	vb[3] = (val >> 24) & 0xFF;
	vb[4] = (val >> 32) & 0xFF;
	vb[5] = (val >> 40) & 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

static packetbuf *
push_int8(packetbuf *buf, long long int val)
{
	char vb[8];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) & 0xFF;
	vb[2] = (val >> 16) & 0xFF;
	vb[3] = (val >> 24) & 0xFF;
	vb[4] = (val >> 32) & 0xFF;
	vb[5] = (val >> 40) & 0xFF;
	vb[6] = (val >> 48) & 0xFF;
	vb[7] = ((unsigned long long int)val >> 56) & 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

static packetbuf *
push_length_int(packetbuf *buf, long long int val)
{
	if (val < 251) {
		push_int1(buf, (char)val);
	} else if (val < 65536) {
		push_int1(buf, '\xfc');
		push_int2(buf, (short)val);
	} else if (val < 16777216) {
		push_int1(buf, '\xfd');
		push_int3(buf, (int)val);
	} else {
		push_int1(buf, '\xfe');
		push_int8(buf, val);
	}

	return buf;
}


static packetbuf *
push_fixed_string(packetbuf *buf, int len, char *str)
{
	return packetbuf_push(buf, str, len);
}

static packetbuf *
push_string(packetbuf *buf, char *str)
{
	return packetbuf_push(buf, str, strlen(str) + 1);
}

static packetbuf *
push_length_string(packetbuf *buf, int len, char *str)
{
	push_length_int(buf, len);
	push_fixed_string(buf, len, str);

	return buf;
}

static char
shift_int1(packetbuf *buf)
{
	char ret;

	if (packetbuf_shift(buf, &ret, 1) == NULL)
		return 0;

	return ret;
}

static short
shift_int2(packetbuf *buf)
{
	unsigned char ret[2];

	if (packetbuf_shift(buf, ret, sizeof(ret)) == NULL)
		return 0;

	return ret[0] + (ret[1] << 8);
}

static int
shift_int3(packetbuf *buf)
{
	unsigned char ret[3];

	if (packetbuf_shift(buf, ret, sizeof(ret)) == NULL)
		return 0;

	return ret[0] + (ret[1] << 8) + (ret[2] << 16);
}

static int
shift_int4(packetbuf *buf)
{
	unsigned char ret[4];

	if (packetbuf_shift(buf, ret, sizeof(ret)) == NULL)
		return 0;

	return ret[0] + (ret[1] << 8) + (ret[2] << 16) + (ret[3] << 24);
}

static long long int
shift_int6(packetbuf *buf)
{
	unsigned char ret[6];

	if (packetbuf_shift(buf, ret, sizeof(ret)) == NULL)
		return 0;

	return ret[0] +
		(ret[1] << 8) +
		(ret[2] << 16) +
		(ret[3] << 24) +
		(((unsigned long long int)ret[4]) << 32) +
		(((unsigned long long int)ret[5]) << 40);
}

static long long int
shift_int8(packetbuf *buf)
{
	unsigned char ret[8];

	if (packetbuf_shift(buf, ret, sizeof(ret)) == NULL)
		return 0;

	return ret[0] +
		(ret[1] << 8) +
		(ret[2] << 16) +
		(ret[3] << 24) +
		(((unsigned long long int)ret[4]) << 32) +
		(((unsigned long long int)ret[5]) << 40) +
		(((unsigned long long int)ret[6]) << 48) +
		(((unsigned long long int)ret[7]) << 56);
}

static long long int
shift_length_int(packetbuf *buf)
{
	char size = shift_int1(buf);

	if (size == '\xfc') {
		return shift_int2(buf);
	} else if (size == '\xfd') {
		return shift_int3(buf);
	} else if (size == '\xfe') {
		return shift_int8(buf);
	}

	return size;
}

static char *
shift_fixed_string(packetbuf *buf, int len)
{
	char *ret = malloc(sizeof(char) * (len + 1));

	if (packetbuf_shift(buf, ret, len) == NULL) {
		free(ret);
		return NULL;
	}

	ret[len] = '\0';  /* convenience */

	return ret;
}

static char *
shift_string(packetbuf *buf)
{
	size_t len = 0;
	unsigned char *p;

	/* a bit ugly, but we need to find the length of the NULL-terminated
	 * string, sort of efficiently */
	for (p = buf->pos; len < buf->len && *p != '\0'; p++, len++)
		;
	len++;  /* '\0' */

	p = malloc(sizeof(char) * len);
	if (packetbuf_shift(buf, p, len) == NULL) {
		free(p);
		return NULL;
	}

	return (char *)p;
}

static char *
shift_length_string(packetbuf *buf)
{
	long long int len;

	len = shift_length_int(buf);
	return shift_fixed_string(buf, (int)len);
}


void
send_handshakev10(int fd, char seq, connprops *props)
{
	packetbuf *buf = packetbuf_get();

	push_int1(buf, 0x0a);  /* protocol version */
	push_string(buf, props->sver);  /* server version */
	push_int4(buf, props->connid);  /* connection ID */
	push_fixed_string(buf, 8, props->chal);  /* auth_plugin_data_part_1 */
	push_int1(buf, 0);  /* filler */
	push_int2(buf, props->capabilities);  /* capability_flags_1 */
	push_int1(buf, props->charset);  /* character_set */
	push_int2(buf, props->status);  /* status_flags: auto-commit */
	push_int2(buf, props->capabilities >> 16);  /* capability_flags_2 */
	push_int1(buf, strlen(props->chal) + 1);  /* length of auth-plugin-data */
	push_int8(buf, 0); push_int2(buf, 0);  /* reserved 10x00 */
	push_string(buf, &props->chal[8]);  /* auth-plugin-data-part-2 */
	push_string(buf, props->auth);  /* auth-plugin-name */

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send handshakev10: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

connprops *
recv_handshakev10(packetbuf *buf, connprops *conn)
{
	char *auth1;
	char *auth2 = NULL;
	char authlen;

	if (shift_int1(buf) != 0x0a)
		return NULL;
	conn->sver = shift_string(buf);
	conn->connid = shift_int4(buf);
	auth1 = shift_fixed_string(buf, 8);
	shift_int1(buf);
	conn->capabilities = shift_int2(buf);
	conn->charset = shift_int1(buf);
	conn->status = shift_int2(buf);
	conn->capabilities |= ((int)shift_int2(buf)) << 16;
	authlen = shift_int1(buf);
	shift_int8(buf); shift_int2(buf);
	if (conn->capabilities & CLIENT_SECURE_CONNECTION) {
		auth2 = shift_fixed_string(buf, authlen > 8 ? authlen - 8 : 0);
	}
	if (conn->capabilities & CLIENT_PLUGIN_AUTH) {
		conn->auth = shift_string(buf);
	}
	if (auth2 != NULL) {
		conn->chal = malloc(sizeof(char) * authlen);
		snprintf(conn->chal, authlen, "%s%s", auth1, auth2);
		free(auth1);
		free(auth2);
	} else {
		conn->chal = auth1;
	}

#ifdef DEBUG
	printf("server %s connection %d, capabilities:",
			conn->sver, conn->connid);
	PRINT_ALL_CAPABILITIES(conn->capabilities);
	printf("\n");
#endif

	return conn;
}

void
send_handshakeresponsev41(int fd, char seq, connprops *props)
{
	packetbuf *buf = packetbuf_get();
	SHA_CTX c;
	unsigned char digest[SHA_DIGEST_LENGTH];
	unsigned char pdigest[SHA_DIGEST_LENGTH];
	int i;

	if (props->dbname == NULL)
		props->capabilities &= ~CLIENT_CONNECT_WITH_DB;

	/* calculate sha1(passwd) xor sha1(<chal>sha1(sha1(passwd))) */
	SHA1_Init(&c);
	SHA1_Update(&c, props->passwd, strlen(props->passwd));
	SHA1_Final(pdigest, &c);
	SHA1_Init(&c);
	SHA1_Update(&c, pdigest, sizeof(pdigest));
	SHA1_Final(digest, &c);
	SHA1_Init(&c);
	SHA1_Update(&c, props->chal, strlen(props->chal));
	SHA1_Update(&c, digest, sizeof(digest));
	SHA1_Final(digest, &c);

	for (i = 0; i < sizeof(digest); i++)
		pdigest[i] ^= digest[i];

	push_int4(buf, props->capabilities);
	push_int4(buf, props->maxpktsize);
	push_int1(buf, props->charset);
	push_int8(buf, 0); push_int8(buf, 0); push_int6(buf, 0); push_int1(buf, 0);
	push_string(buf, props->username);
	if (props->capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
		push_length_int(buf, sizeof(pdigest));
		push_fixed_string(buf, sizeof(pdigest), (char *)pdigest);
	} else if (props->capabilities & CLIENT_SECURE_CONNECTION) {
		push_int1(buf, sizeof(digest));
		push_fixed_string(buf, sizeof(pdigest), (char *)pdigest);
	} else {
		push_fixed_string(buf, sizeof(pdigest), (char *)pdigest);
		push_int1(buf, 0);
	}
	if (props->capabilities & CLIENT_CONNECT_WITH_DB) {
		push_string(buf, props->dbname);
	}
	if (props->capabilities & CLIENT_PLUGIN_AUTH) {
		push_string(buf, props->auth);
	}
	if (props->capabilities & CLIENT_CONNECT_ATTRS) {
		push_length_int(buf,
				1 + strlen("_client_name") +
				1 + strlen("mysqlpq") +
				1 + strlen("_client_version") +
				1 + strlen(VERSION));
		push_length_string(buf, strlen("_client_name"), "_client_name");
		push_length_string(buf, strlen("mysqlpq"), "mysqlpq");
		push_length_string(buf, strlen("_client_version"), "_client_version");
		push_length_string(buf, strlen(VERSION), VERSION);
	}

#ifdef DEBUG
	printf("connecting to server using capabilities:");
	PRINT_ALL_CAPABILITIES(props->capabilities);
	printf("\n");
#endif

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send handshakeresponsev41: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

int
recv_handshakeresponsev41(packetbuf *buf, connprops *props)
{
	int capabilities = shift_int2(buf);

	if ((capabilities & CLIENT_PROTOCOL_41) == 0) {
		fprintf(stderr, "client not v41 :(\n");
		/* FIXME: b0rk here */
		return 0;
	}

	props->capabilities =
		capabilities += (shift_int2(buf) >> 16);  /* 4 bytes in v41 */

#ifdef DEBUG
	printf("capabilities:");
	PRINT_ALL_CAPABILITIES(capabilities);
	printf("\n");
#endif

	props->maxpktsize = shift_int4(buf);
	props->charset = shift_int1(buf);
	shift_int8(buf); shift_int8(buf); shift_int6(buf); shift_int1(buf);
	if (props->username != NULL)
		free(props->username);
	props->username = shift_string(buf);

	if (props->chalresponse != NULL)
		free(props->chalresponse);
	if (capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
		long long int authlen = shift_length_int(buf);
		props->chalresponse = shift_fixed_string(buf, authlen);
	} else if (capabilities & CLIENT_SECURE_CONNECTION) {
		char authlen = shift_int1(buf);
		props->chalresponse = shift_fixed_string(buf, authlen);
	} else {
		props->chalresponse = shift_string(buf);
	}
	if (capabilities & CLIENT_CONNECT_WITH_DB) {
		if (props->dbname != NULL)
			free(props->dbname);
		props->dbname = shift_string(buf);
	}
	if (capabilities & CLIENT_PLUGIN_AUTH) {
		if (props->auth != NULL)
			free(props->auth);
		props->auth = shift_string(buf);
	}
	if (capabilities & CLIENT_CONNECT_ATTRS) {
		long long int len = shift_length_int(buf);  /* keyslen */
		props->attrs = shift_fixed_string(buf, len);
	}

	return 1;
}

char *
recv_comquery(packetbuf *buf)
{
	shift_int1(buf);
	return shift_fixed_string(buf, packetbuf_hdr_len(buf) - 1);
}

void
send_ok(int fd, char seq, int capabilities, char *info)
{
	packetbuf *buf = packetbuf_get();
	
	if (info == NULL)
		info = "";

	push_int1(buf, MYSQL_OK);  /* OK packet header */
	push_length_int(buf, 0);  /* affected rows */
	push_length_int(buf, 0);  /* last inserted id */
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		push_int2(buf, 0x0002);  /* auto-commit */
		push_int2(buf, 0);  /* warnings */
	}
	if (capabilities & CLIENT_SESSION_TRACK) {
		push_length_string(buf, strlen(info), info);
	} else {
		push_fixed_string(buf, strlen(info), info);
	}

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send ok: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

mysql_ok *
recv_ok(packetbuf *buf, int capabilities)
{
	mysql_ok *ret = malloc(sizeof(mysql_ok));

	shift_int1(buf);  /* 0x00 */
	ret->affrows = shift_length_int(buf);  /* affected rows */
	ret->lastid = shift_length_int(buf);  /* last insert-id */
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		ret->status_flags = shift_int2(buf);
		ret->warnings = shift_int2(buf);
	} else if (capabilities & CLIENT_TRANSACTIONS) {
		ret->status_flags = shift_int2(buf);
		ret->warnings = 0;
	} else {
		ret->status_flags = 0;
		ret->warnings = 0;
	}
	if (capabilities & CLIENT_SESSION_TRACK) {
		ret->status_info = shift_length_string(buf);
		if (ret->status_flags & SERVER_SESSION_STATE_CHANGED) {
			ret->session_state_info = shift_length_string(buf);
		} else {
			ret->session_state_info = NULL;
		}
	} else {
		size_t avail = buf->len - (buf->pos - buf->buf);
		ret->status_info = shift_fixed_string(buf, avail);
		ret->session_state_info = NULL;
	}

	return ret;
}

mysql_eof *
recv_eof(packetbuf *buf, int capabilities)
{
	mysql_eof *ret = malloc(sizeof(mysql_eof));

	shift_int1(buf);  /* 0xfe */
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		ret->warnings = shift_int2(buf);
		ret->status_flags = shift_int2(buf);
	} else {
		ret->warnings = 0;
		ret->status_flags = 0;
	}

	return ret;
}

void
send_err(int fd, char seq, int capabilities, char *sqlstate, char *msg)
{
	packetbuf *buf = packetbuf_get();

	push_int1(buf, MYSQL_ERR);  /* ERR packet header */
	push_int2(buf, 1105);  /* error code: ER_UNKNOWN_ERROR */
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		char sbuf[6];
		int i;
		memset(sbuf, '\0', 6);
		sbuf[0] = '#';
		for (i = 1; i < sizeof(sbuf) && *sqlstate != '\0'; sqlstate++, i++)
			sbuf[i] = *sqlstate;
		push_fixed_string(buf, 6, sbuf);
	}
	push_fixed_string(buf, strlen(msg), msg);

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send err: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

void
send_eof_str(int fd, char seq, char *msg)
{
	packetbuf *buf = packetbuf_get();

	push_fixed_string(buf, strlen(msg), msg);

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send eof str: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

char *
recv_err(packetbuf *buf, int capabilities)
{
	short errcode;
	char *errmsg;

	shift_int1(buf);  /* MYSQL_ERR */
	errcode = shift_int2(buf);
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		errmsg = shift_fixed_string(buf, buf->len - 3);
		/* shift the sqlstate to make it human consumable */
		memmove(errmsg, errmsg + 1, 5);
		errmsg[5] = ':';
	} else {
		errmsg = shift_fixed_string(buf, buf->len - 3);
	}

	return errmsg;
}
