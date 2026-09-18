CC ?= cc
CFLAGS ?= -O2 -g -std=c11 -Wall -Wextra -Wpedantic
CPPFLAGS += -Iinclude
LDLIBS ?= -pthread

.PHONY: all clean test

all: echo_server

echo_server: src/epoll_server.o examples/echo_server.o
	$(CC) $(CFLAGS) $^ $(LDLIBS) -o $@

src/epoll_server.o: src/epoll_server.c include/epoll_server.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

examples/echo_server.o: examples/echo_server.c include/epoll_server.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

test: echo_server
	./tests/integration.sh

clean:
	$(RM) echo_server src/*.o examples/*.o
