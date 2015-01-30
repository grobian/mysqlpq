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

#ifndef HAVE_MYSQLPROTO_H
#define HAVE_MYSQLPROTO_H 1

#define COM_QUIT_STR "\x01\x00\x00\x00\x01"

#define COM_QUIT       0x01
#define COM_INIT_DB    0x02
#define COM_QUERY      0x03
#define COM_REFRESH    0x07
#define COM_STATISTICS 0x09
#define COM_CONNECT    0x0b
#define COM_PING       0x0e

#define MYSQL_OK       0x00
#define MYSQL_ERR      0xff
#define MYSQL_EOF      0xfe

#define CLIENT_LONG_PASSWORD         1 /* 0x01 new more secure passwords */
#define CLIENT_FOUND_ROWS            2 /* 0x02 Found instead of affected rows */
#define CLIENT_LONG_FLAG             4 /* 0x04 Get all column flags */
#define CLIENT_CONNECT_WITH_DB       8 /* 0x08 One can specify db on connect */
#define CLIENT_NO_SCHEMA            16 /* 0x10 Don't allow database.table.column */
#define CLIENT_COMPRESS             32 /* 0x20 Can use compression protocol */
#define CLIENT_ODBC                 64 /* 0x40 Odbc client */
#define CLIENT_LOCAL_FILES         128 /* 0x80 Can use LOAD DATA LOCAL */
#define CLIENT_IGNORE_SPACE        256 /* 0x0100 Ignore spaces before '(' */
#define CLIENT_PROTOCOL_41         512 /* 0x0200 New 4.1 protocol */
#define CLIENT_INTERACTIVE        1024 /* 0x0400 This is an interactive client */
#define CLIENT_SSL                2048 /* 0x0800 Switch to SSL after handshake */
#define CLIENT_IGNORE_SIGPIPE     4096 /* 0x1000 IGNORE sigpipes */
#define CLIENT_TRANSACTIONS       8192 /* 0x2000 Client knows about transactions */
#define CLIENT_RESERVED          16384 /* 0x4000 Old flag for 4.1 protocol  */
#define CLIENT_SECURE_CONNECTION 32768 /* 0x8000 New 4.1 authentication */
#define CLIENT_MULTI_STATEMENTS (1UL << 16) /* 0x010000 Enable/disable multi-stmt support */
#define CLIENT_MULTI_RESULTS    (1UL << 17) /* 0x020000 Enable/disable multi-results */
#define CLIENT_PS_MULTI_RESULTS (1UL << 18) /* 0x040000 Multi-results in PS-protocol */

#define CLIENT_PLUGIN_AUTH      (1UL << 19) /* 0x080000 Client supports plugin authentication */
#define CLIENT_CONNECT_ATTRS    (1UL << 20) /* 0x100000 Client supports connection attributes */

/* Enable authentication response packet to be larger than 255 bytes. */
#define CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA (1UL << 21) /* 0x200000 */

/* Don't close the connection for a connection with expired password. */
#define CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS (1UL << 22) /* 0x400000 */
#define CLIENT_SESSION_TRACK (1UL << 23)    /* 0x800000 */

#define CLIENT_DEPRECATE_EOF    (1UL << 24) /* 0x01000000 Expects OK iso EOF */

#define CLIENT_SSL_VERIFY_SERVER_CERT (1UL << 30) /* 0x40000000 */
#define CLIENT_REMEMBER_OPTIONS (1UL << 31) /* 0x080000000 */

#define CAN_CLIENT_COMPRESS 0

/* Gather all possible capabilites (flags) supported by the server */
#define CLIENT_ALL_FLAGS  (CLIENT_LONG_PASSWORD \
                           | CLIENT_FOUND_ROWS \
                           | CLIENT_LONG_FLAG \
                           | CLIENT_CONNECT_WITH_DB \
                           | CLIENT_NO_SCHEMA \
                           | CLIENT_COMPRESS \
                           | CLIENT_ODBC \
                           | CLIENT_LOCAL_FILES \
                           | CLIENT_IGNORE_SPACE \
                           | CLIENT_PROTOCOL_41 \
                           | CLIENT_INTERACTIVE \
                           | CLIENT_SSL \
                           | CLIENT_IGNORE_SIGPIPE \
                           | CLIENT_TRANSACTIONS \
                           | CLIENT_RESERVED \
                           | CLIENT_SECURE_CONNECTION \
                           | CLIENT_MULTI_STATEMENTS \
                           | CLIENT_MULTI_RESULTS \
                           | CLIENT_PS_MULTI_RESULTS \
                           | CLIENT_SSL_VERIFY_SERVER_CERT \
                           | CLIENT_REMEMBER_OPTIONS \
                           | CLIENT_PLUGIN_AUTH \
                           | CLIENT_CONNECT_ATTRS \
                           | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA \
                           | CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS \
                           | CLIENT_DEPRECATE_EOF \
                           | CLIENT_REMEMBER_OPTIONS \
)

#define CLIENT_BASIC_FLAGS (((CLIENT_ALL_FLAGS & ~CLIENT_SSL) \
                                               & ~CLIENT_COMPRESS) \
                                               & ~CLIENT_SSL_VERIFY_SERVER_CERT)

/* these are Protocol::StatusFlags */
#define SERVER_STATUS_IN_TRANS        0x0001
#define SERVER_STATUS_AUTOCOMMIT      0x0002
#define SERVER_MORE_RESULTS_EXISTS    0x0008
#define SERVER_SESSION_STATE_CHANGED  0x4000

typedef struct {
	unsigned char *buf;
	unsigned char *pos;
	size_t size;
	ssize_t len;
} packetbuf;

typedef struct {
	int capabilities;
	char *sver;
	int connid;
	char *chal;
	char *auth;
	char charset;
	short status;
	int maxpktsize;
	char *username;
	char *passwd;
	char *chalresponse;
	char *dbname;
	char *attrs;
} connprops;

typedef struct {
	long long int affrows;
	long long int lastid;
	short status_flags;
	short warnings;
	char *status_info;
	char *session_state_info;
} mysql_ok;

typedef struct {
	short status_flags;
	short warnings;
} mysql_eof;

int packetbuf_recv_data(packetbuf **buf, int fd);
int packetbuf_send(packetbuf *buf, char seq, int fd);
int packetbuf_forward(packetbuf *buf, int fd);
void packetbuf_free(packetbuf *buf);

int packetbuf_hdr_len(packetbuf *buf);
char packetbuf_hdr_seq(packetbuf *buf);

void send_handshakev10(int fd, char seq, connprops *props);
connprops *recv_handshakev10(packetbuf *buf, connprops *props);
int recv_handshakeresponsev41(packetbuf *buf, connprops *props);
void send_handshakeresponsev41(int fd, char seq, connprops *props);
char *recv_comquery(packetbuf *buf);

void send_ok(int fd, char seq, int capabilities, mysql_ok *data);
mysql_ok *recv_ok(packetbuf *buf, int capabilities);
void send_eof(int fd, char seq, int capabillities, mysql_eof *data);
mysql_eof *recv_eof(packetbuf *buf, int capabilities);
void send_eof_str(int fd, char seq, char *msg);
char *recv_eof_str(packetbuf *buf);
void send_err(int fd, char seq, int capabilities, char *code, char *msg);
char *recv_err(packetbuf *buf, int capabilities);
long long int recv_field_count(packetbuf *buf);

#endif
