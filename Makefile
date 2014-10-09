# Copyright 2013-2014 Fabian Groffen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


CFLAGS ?= -O2 -Wall

GIT_VERSION := $(shell git describe --abbrev=6 --dirty --always || date +%F)
GVCFLAGS += -DGIT_VERSION=\"$(GIT_VERSION)\"

override CFLAGS += $(GVCFLAGS) `pkg-config openssl --cflags` -pthread

SOCKET_LIBS =
ifeq ($(shell uname), SunOS)
SOCKET_LIBS += -lsocket  -lnsl
endif

override LIBS += `pkg-config openssl --libs` $(SOCKET_LIBS) -pthread

OBJS = \
	mysqlpq.o \
	receptor.o \
	dispatcher.o \
	mysqlproto.o

mysqlpq: $(OBJS)
	$(CC) -o $@ $(LDFLAGS) $^ $(LIBS)

VERSION = $(shell sed -n '/VERSION/s/^.*"\([0-9.]\+\)".*$$/\1/p' mysqlpq.h)
dist:
	git archive \
		--format=tar.gz \
		--prefix=mysqlpq-$(VERSION)/ v$(VERSION) \
		> mysqlpq-$(VERSION).tar.gz

clean:
	rm -f *.o mysqlpq
