#ifndef EPOLL_SERVER_H
#define EPOLL_SERVER_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct epoll_server epoll_server_t;
typedef struct epoll_connection epoll_connection_t;

typedef struct {
    const char *bind_address; /* NULL means all IPv4 interfaces. */
    uint16_t port;
    int backlog;
    size_t worker_count;
    size_t max_events;
    bool use_writev; /* Batch queued buffers with writev when true. */
} epoll_server_config_t;

typedef struct {
    void (*on_open)(epoll_connection_t *connection, void *user_data);
    void (*on_data)(epoll_connection_t *connection,
                    const unsigned char *data,
                    size_t size,
                    void *user_data);
    void (*on_close)(epoll_connection_t *connection, void *user_data);
} epoll_server_callbacks_t;

epoll_server_t *epoll_server_create(const epoll_server_config_t *config,
                                    const epoll_server_callbacks_t *callbacks,
                                    void *user_data);
int epoll_server_run(epoll_server_t *server);
void epoll_server_stop(epoll_server_t *server);
void epoll_server_destroy(epoll_server_t *server);

int epoll_connection_send(epoll_connection_t *connection,
                          const void *data,
                          size_t size);
void epoll_connection_close(epoll_connection_t *connection);
int epoll_connection_fd(const epoll_connection_t *connection);

#ifdef __cplusplus
}
#endif

#endif
