#include "epoll_server.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static epoll_server_t *running_server;

static void stop_server(int signal_number) {
    (void)signal_number;
    epoll_server_stop(running_server);
}

static void echo_data(epoll_connection_t *connection,
                      const unsigned char *data,
                      size_t size,
                      void *user_data) {
    (void)user_data;
    if (epoll_connection_send(connection, data, size) != 0)
        epoll_connection_close(connection);
}

int main(int argc, char **argv) {
    uint16_t port = argc > 1 ? (uint16_t)strtoul(argv[1], NULL, 10) : 9000;
    epoll_server_config_t config = {
        .bind_address = "127.0.0.1",
        .port = port,
        .backlog = 256,
        .worker_count = 4,
        .max_events = 256,
        .use_writev = argc > 2 && strtoul(argv[2], NULL, 10) != 0,
    };
    epoll_server_callbacks_t callbacks = {
        .on_data = echo_data,
        .on_priority_data = echo_data,
    };
    running_server = epoll_server_create(&config, &callbacks, NULL);
    if (!running_server) {
        perror("epoll_server_create");
        return 1;
    }
    signal(SIGINT, stop_server);
    signal(SIGTERM, stop_server);
    printf("echo server listening on 127.0.0.1:%u\n", port);
    int result = epoll_server_run(running_server);
    epoll_server_destroy(running_server);
    return result == 0 ? 0 : 1;
}
