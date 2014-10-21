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

#ifndef HAVE_MYSQLPROTO_H
#define HAVE_MYSQLPROTO_H 1

#define COM_QUIT_STR "\x01\x00\x00\x00\x01"

#define COM_QUIT     0x01
#define COM_QUERY    0x03
#define COM_CONNECT  0x0b
#define COM_PING     0x0e

#define CLIENT_LONG_PASSWORD    1   /* new more secure passwords */
#define CLIENT_FOUND_ROWS   2   /* Found instead of affected rows */
#define CLIENT_LONG_FLAG    4   /* Get all column flags */
#define CLIENT_CONNECT_WITH_DB  8   /* One can specify db on connect */
#define CLIENT_NO_SCHEMA    16  /* Don't allow database.table.column */
#define CLIENT_COMPRESS     32  /* Can use compression protocol */
#define CLIENT_ODBC     64  /* Odbc client */
#define CLIENT_LOCAL_FILES  128 /* Can use LOAD DATA LOCAL */
#define CLIENT_IGNORE_SPACE 256 /* Ignore spaces before '(' */
#define CLIENT_PROTOCOL_41  512 /* New 4.1 protocol */
#define CLIENT_INTERACTIVE  1024    /* This is an interactive client */
#define CLIENT_SSL              2048    /* Switch to SSL after handshake */
#define CLIENT_IGNORE_SIGPIPE   4096    /* IGNORE sigpipes */
#define CLIENT_TRANSACTIONS 8192    /* Client knows about transactions */
#define CLIENT_RESERVED         16384   /* Old flag for 4.1 protocol  */
#define CLIENT_SECURE_CONNECTION 32768  /* New 4.1 authentication */
#define CLIENT_MULTI_STATEMENTS (1UL << 16) /* Enable/disable multi-stmt support */
#define CLIENT_MULTI_RESULTS    (1UL << 17) /* Enable/disable multi-results */
#define CLIENT_PS_MULTI_RESULTS (1UL << 18) /* Multi-results in PS-protocol */

#define CLIENT_PLUGIN_AUTH  (1UL << 19) /* Client supports plugin authentication */
#define CLIENT_CONNECT_ATTRS (1UL << 20) /* Client supports connection attributes */

/* Enable authentication response packet to be larger than 255 bytes. */
#define CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA (1UL << 21)

/* Don't close the connection for a connection with expired password. */
#define CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS (1UL << 22)

#define CLIENT_SSL_VERIFY_SERVER_CERT (1UL << 30)
#define CLIENT_REMEMBER_OPTIONS (1UL << 31)

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
)

#define CLIENT_BASIC_FLAGS (((CLIENT_ALL_FLAGS & ~CLIENT_SSL) \
                                               & ~CLIENT_COMPRESS) \
                                               & ~CLIENT_SSL_VERIFY_SERVER_CERT)

typedef struct {
	unsigned char *buf;
	unsigned char *pos;
	size_t size;
	size_t len;
} packetbuf;

packetbuf *packetbuf_recv_hdr(int fd);
int packetbuf_recv_data(packetbuf *buf, int fd);
int packetbuf_send(packetbuf *buf, char seq, int fd);
void packetbuf_free(packetbuf *buf);

int packetbuf_hdr_len(packetbuf *buf);
char packetbuf_hdr_seq(packetbuf *buf);

void send_handshakev10(int fd, char seq);
int recv_handshakeresponsev41(packetbuf *buf);
char *recv_comquery(packetbuf *buf);

void send_ok(int fd, char seq, int capabilities);
void send_err(int fd, char seq, int capabilities, char *code, char *msg);

#endif