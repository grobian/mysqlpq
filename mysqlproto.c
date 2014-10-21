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

static packetbuf *
packetbuf_reset(packetbuf *buf)
{
	buf->pos = buf->buf;
	buf->len = 0;

	return buf;
}

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

	return write(fd, hdr, sizeof(mysql_packet_hdr) + buf->len);
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
send_handshakev10(int fd, char seq)
{
	packetbuf *buf = packetbuf_get();
	int flags = CLIENT_BASIC_FLAGS;

	push_int1(buf, 0x0a);  /* protocol version */
	push_string(buf, "5.7-mysqlpq-" VERSION);  /* server version */
	push_int4(buf, 1);  /* connection ID */
	push_fixed_string(buf, 8, "12345678");  /* auth_plugin_data_part_1 */
	push_int1(buf, 0);  /* filler */
	push_int2(buf, flags);  /* capability_flags_1 */
	push_int1(buf, 0x21);  /* character_set */
	push_int2(buf, 0x0002);  /* status_flags: auto-commit */
	push_int2(buf, flags >> 16);  /* capability_flags_2 */
	push_int1(buf, 21);  /* length of auth-plugin-data */
	push_int8(buf, 0); push_int2(buf, 0);  /* reserved 10x00 */
	push_fixed_string(buf, 13, "123456789012");  /* auth-plugin-data-part-2 */
	push_string(buf, "mysql_native_password");  /* auth-plugin-name */

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send handshakev10: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

int
recv_handshakev10(packetbuf *buf)
{
	char *ver;
	int id;
	char *auth1;
	char *auth2 = NULL;
	char authlen;
	int capabilities;
	char charset;
	short status;
	char *plugin = NULL;

	if (shift_int1(buf) != 0x0a)
		return 0;
	ver = shift_string(buf);
	id = shift_int4(buf);
	printf("connected to mysql %s, connection id: %d\n", ver, id);
	auth1 = shift_fixed_string(buf, 8);
	shift_int1(buf);
	capabilities = shift_int2(buf);
	charset = shift_int1(buf);
	status = shift_int2(buf);
	capabilities |= ((int)shift_int2(buf)) << 16;
	authlen = shift_int1(buf);
	shift_int8(buf); shift_int2(buf);
	if (capabilities & CLIENT_SECURE_CONNECTION) {
		auth2 = shift_fixed_string(buf, authlen > 8 ? authlen - 8 : 0);
	}
	if (capabilities & CLIENT_PLUGIN_AUTH) {
		plugin = shift_string(buf);
	}

	return capabilities;
}

void
send_handshakeresponsev41(int fd, char seq, char *chal, int challen, char *user, char *passwd)
{
	packetbuf *buf = packetbuf_get();
	SHA_CTX c;
	unsigned char digest[SHA_DIGEST_LENGTH];
	int capabilities = CLIENT_BASIC_FLAGS & ~CLIENT_CONNECT_WITH_DB;
	char *dbname = NULL;

	/* calculate sha1(<chal>sha1(sha1(passwd))) */
	SHA1_Init(&c);
	SHA1_Update(&c, passwd, strlen(passwd));
	SHA1_Final(digest, &c);
	SHA1_Init(&c);
	SHA1_Update(&c, digest, sizeof(digest));
	SHA1_Final(digest, &c);
	SHA1_Init(&c);
	SHA1_Update(&c, chal, challen);
	SHA1_Update(&c, digest, sizeof(digest));
	SHA1_Final(digest, &c);

	push_int4(buf, capabilities);
	push_int4(buf, 0xFEFFFFFF);
	push_int1(buf, 0x33);
	push_int8(buf, 0); push_int8(buf, 0); push_int6(buf, 0); push_int1(buf, 0);
	push_string(buf, user);
	push_length_int(buf, sizeof(digest));
	push_fixed_string(buf, sizeof(digest), (char *)digest);
	if (capabilities & CLIENT_CONNECT_WITH_DB) {
		push_string(buf, dbname);
	}
	push_string(buf, "mysql_native_password");
}

int
recv_handshakeresponsev41(packetbuf *buf)
{
	int capabilities = shift_int2(buf);

	if ((capabilities & CLIENT_PROTOCOL_41) == 0) {
		fprintf(stderr, "client not v41 :(\n");
		/* FIXME: b0rk here */
		return 0;
	}

	capabilities += (shift_int2(buf) >> 16);  /* 4 bytes in v41 */

#ifdef DEBUG
	printf("capabilities:");
	if (capabilities & CLIENT_PROTOCOL_41)
		printf(" CLIENT_PROTOCOL_41");
	if (capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
		printf(" CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA");
	if (capabilities & CLIENT_SECURE_CONNECTION)
		printf(" CLIENT_SECURE_CONNECTION");
	if (capabilities & CLIENT_CONNECT_WITH_DB)
		printf(" CLIENT_CONNECT_WITH_DB");
	if (capabilities & CLIENT_PLUGIN_AUTH)
		printf(" CLIENT_PLUGIN_AUTH");
	if (capabilities & CLIENT_CONNECT_ATTRS)
		printf(" CLIENT_CONNECT_ATTRS");
	printf("\n");
#endif

	int maxpktsize = shift_int4(buf);
	char charset = shift_int1(buf);
	char *filler = shift_fixed_string(buf, 23);
	char *username = shift_string(buf);
	char *authresponse;
	char *database = NULL;
	char *authpluginname = NULL;

	if (capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
		long long int authlen = shift_length_int(buf);
		authresponse = shift_fixed_string(buf, authlen);
	} else if (capabilities & CLIENT_SECURE_CONNECTION) {
		char authlen = shift_int1(buf);
		authresponse = shift_fixed_string(buf, authlen);
	} else {
		authresponse = shift_string(buf);
	}
	if (capabilities & CLIENT_CONNECT_WITH_DB) {
		database = shift_string(buf);
	}
	if (capabilities & CLIENT_PLUGIN_AUTH) {
		authpluginname = shift_string(buf);
	}
	if (capabilities & CLIENT_CONNECT_ATTRS) {
		char *d;
		shift_length_int(buf);  /* keyslen */
		while (1) {
			/* this is keys and values handled as one */
			d = shift_length_string(buf);
			if (d == NULL)
				break;
			free(d);
		}
	}

	printf("request from %s, charset %d\n", username, charset);
	free(filler);
	free(username);
	free(authresponse);
	if (database != NULL)
		free(database);
	if (authpluginname != NULL)
		free(authpluginname);

	return capabilities;
}

char *
recv_comquery(packetbuf *buf)
{
	shift_int1(buf);
	return shift_fixed_string(buf, packetbuf_hdr_len(buf) - 1);
}

void
send_ok(int fd, char seq, int capabilities)
{
	packetbuf *buf = packetbuf_get();
	char *info = "congratulations, you just logged in";

	push_int1(buf, 0);  /* OK packet header */
	push_length_int(buf, 0);  /* affected rows */
	push_length_int(buf, 0);  /* last inserted id */
	if (capabilities & CLIENT_PROTOCOL_41) {  /* must be */
		push_int2(buf, 0x0002);  /* auto-commit */
		push_int2(buf, 0);  /* warnings */
	}
	if (capabilities & 0x00800000) {  /* CLIENT_SESSION_TRACK */
		push_length_string(buf, strlen(info), info);
	} else {
		push_fixed_string(buf, strlen(info), info);
	}

	if (packetbuf_send(buf, seq, fd) == -1) {
		fprintf(stderr, "failed to send ok: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}

void
send_err(int fd, char seq, int capabilities, char *sqlstate, char *msg)
{
	packetbuf *buf = packetbuf_get();

	push_int1(buf, 0xFF);  /* ERR packet header */
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
