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

/* lame implementation of just about what we need */

typedef struct {
	char len[3];
	char seq;
} mysql_packet_hdr;

typedef struct {
	char *buf;
	char *pos;
	size_t size;
	size_t len;
} packetbuf;

#define COM_QUIT_STR "\x01\x00\x00\x00\x01"

#define COM_QUIT     0x01
#define COM_QUERY    0x03
#define COM_CONNECT  0x0b
#define COM_PING     0x0e


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
		char *newbuf = realloc(
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

static void
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

static int
packetbuf_send(packetbuf *buf, int fd)
{
	/* fill in the reserved bytes before buf */
	mysql_packet_hdr *hdr = (void *)(buf->buf - sizeof(mysql_packet_hdr));

	/* we only support single packets, but we could loop here and
	 * increment seq, yada yada */
	hdr->len[0] = buf->len & 0xFF;
	hdr->len[1] = (buf->len >> 8) & 0xFF;
	hdr->len[2] = (buf->len >> 16) & 0xFF;
	hdr->seq = 0x00;

	return write(fd, hdr, sizeof(mysql_packet_hdr) + buf->len);
}

static packetbuf *
packetbuf_recv(int fd)
{
	packetbuf *ret = packetbuf_get();
	mysql_packet_hdr *hdr = (void *)(ret->buf - sizeof(mysql_packet_hdr));

	if (read(fd, hdr, sizeof(mysql_packet_hdr)) != sizeof(mysql_packet_hdr)) {
		fprintf(stderr, "failed to read mysql package header\n");
		packetbuf_free(ret);
		return NULL;
	}

	/* we don't support sequences here, just assume all is in one pkg */
	ret->len = hdr->len[0] + (hdr->len[1] << 8) + (hdr->len[2] << 16);

	packetbuf_realloc(ret, ret->len);
	if (read(fd, ret->buf, ret->len) != ret->len) {
		fprintf(stderr, "failed to read full mysql package of %zd bytes\n",
				ret->len);
		packetbuf_free(ret);
		return NULL;
	}

	return ret;
}

static packetbuf *
packetbuf_push(packetbuf *buf, void *data, size_t len)
{
	packetbuf_realloc(buf, len);

	memcpy(buf->pos, data, len);
	buf->pos += len;
	buf->len += len;

	return buf;
}

static packetbuf *
packetbuf_shift(packetbuf *buf, void *ptr, size_t len)
{
	if (len > (buf->len - (buf->pos - buf->buf))) {
		return NULL;  /* FIXME: return code? */
	}

	memcpy(ptr, buf->pos, len);
	buf->pos += len;

	return buf;
}


/* all LSB */
packetbuf *
push_int1(packetbuf *buf, char val)
{
	return packetbuf_push(buf, &val, 1);
}

packetbuf *
push_int2(packetbuf *buf, short val)
{
	char vb[2];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) && 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

packetbuf *
push_int3(packetbuf *buf, int val)
{
	char vb[3];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) && 0xFF;
	vb[2] = (val >> 16) && 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

packetbuf *
push_int4(packetbuf *buf, int val)
{
	char vb[4];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) && 0xFF;
	vb[2] = (val >> 16) && 0xFF;
	vb[3] = (val >> 24) && 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

packetbuf *
push_int6(packetbuf *buf, long long int val)
{
	char vb[6];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) && 0xFF;
	vb[2] = (val >> 16) && 0xFF;
	vb[3] = (val >> 24) && 0xFF;
	vb[4] = (val >> 32) && 0xFF;
	vb[5] = (val >> 40) && 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

packetbuf *
push_int8(packetbuf *buf, long long int val)
{
	char vb[8];

	vb[0] = val & 0xFF;
	vb[1] = (val >> 8) && 0xFF;
	vb[2] = (val >> 16) && 0xFF;
	vb[3] = (val >> 24) && 0xFF;
	vb[4] = (val >> 32) && 0xFF;
	vb[5] = (val >> 40) && 0xFF;
	vb[6] = (val >> 48) && 0xFF;
	vb[7] = (val >> 56) && 0xFF;

	return packetbuf_push(buf, vb, sizeof(vb));
}

packetbuf *
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


packetbuf *
push_fixed_string(packetbuf *buf, int len, char *str)
{
	return packetbuf_push(buf, str, len);
}

packetbuf *
push_string(packetbuf *buf, char *str)
{
	return packetbuf_push(buf, str, strlen(str) + 1);
}

packetbuf *
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

	packetbuf_shift(buf, &ret, 1);

	return ret;
}

static short
shift_int2(packetbuf *buf)
{
	char ret[2];

	packetbuf_shift(buf, ret, sizeof(ret));

	return ret[0] + (ret[1] << 8);
}

static int
shift_int3(packetbuf *buf)
{
	char ret[3];

	packetbuf_shift(buf, ret, sizeof(ret));

	return ret[0] + (ret[1] << 8) + (ret[2] << 16);
}

static int
shift_int4(packetbuf *buf)
{
	char ret[4];

	packetbuf_shift(buf, ret, sizeof(ret));

	return ret[0] + (ret[1] << 8) + (ret[2] << 16) + (ret[3] << 24);
}

static long long int
shift_int6(packetbuf *buf)
{
	char ret[6];

	packetbuf_shift(buf, ret, sizeof(ret));

	return ret[0] +
		(ret[1] << 8) +
		(ret[2] << 16) +
		(ret[3] << 24) +
		(((long long int)ret[4]) << 32) +
		(((long long int)ret[5]) << 40);
}

static long long int
shift_int8(packetbuf *buf)
{
	char ret[8];

	packetbuf_shift(buf, ret, sizeof(ret));

	return ret[0] +
		(ret[1] << 8) +
		(ret[2] << 16) +
		(ret[3] << 24) +
		(((long long int)ret[4]) << 32) +
		(((long long int)ret[5]) << 40) +
		(((long long int)ret[6]) << 48) +
		(((long long int)ret[7]) << 56);
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

	packetbuf_shift(buf, ret, len);

	ret[len] = '\0';  /* convenience */

	return ret;
}

static char *
shift_string(packetbuf *buf)
{
	size_t len = 0;
	char *p;

	/* a bit ugly, but we need to find the length of the NULL-terminated
	 * string, sort of efficiently */
	for (p = buf->pos; len < buf->len && *p != '\0'; p++, len++)
		;
	len++;  /* '\0' */

	p = malloc(sizeof(char) * len);
	packetbuf_shift(buf, p, len);

	return p;
}

static char *
shift_length_string(packetbuf *buf)
{
	long long int len;

	len = shift_length_int(buf);
	return shift_fixed_string(buf, (int)len);
}


void
send_handshakev10(int fd)
{
	packetbuf *buf = packetbuf_new();
	int flags;

	push_int1(buf, 0x0a);  /* protocol version */
	push_string(buf, "5.7-mysqlpq");  /* server version */
	push_int4(buf, 0);  /* connection ID */
	push_fixed_string(buf, 8, "12345678");  /* auth_plugin_data_part_1 */
	push_int1(buf, 0);  /* filler */
	flags = 0
		| 0x00000001   /* CLIENT_LONG_PASSWORD */
		| 0x00000200   /* CLIENT_PROTOCOL_41 */
		| 0x00000400   /* CLIENT_INTERACTIVE */
		| 0x00010000   /* CLIENT_MULTI_STATEMENTS */
		| 0x00020000   /* CLIENT_MULTI_RESULTS */
		| 0x00040000   /* CLIENT_PS_MULTI_RESULTS */
		;
	push_int2(buf, flags & 0xFFFF);  /* capability_flags_1 */
	push_int1(buf, 0x08);  /* character_set */
	push_int2(buf, 0x0002);  /* status_flags: auto-commit */
	push_int2(buf, (flags >> 16) && 0xFFFF);  /* capability_flags_2 */
	push_int1(buf, 0);  /* filler */
	push_int8(buf, 0); push_int2(buf, 0);  /* reserved 10x00 */

	if (packetbuf_send(buf, fd) == -1) {
		fprintf(stderr, "failed to send handshakev10: %s\n", strerror(errno));
	}

	packetbuf_free(buf);
}
