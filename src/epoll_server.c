#define _GNU_SOURCE
#include "epoll_server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

enum { CONNECTION_EVENT_MASK = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP };

typedef struct event_item {
    uint32_t events;
    struct event_item *next;
} event_item_t;

typedef struct send_item {
    unsigned char *data;
    size_t size;
    size_t offset;
    struct send_item *next;
} send_item_t;

typedef enum { COMMAND_REFRESH, COMMAND_CLOSE } command_type_t;

typedef struct command {
    command_type_t type;
    struct epoll_connection *connection;
    struct command *next;
} command_t;

struct epoll_connection {
    int fd;
    struct epoll_server *server;
    pthread_mutex_t mutex;
    event_item_t *event_head;
    event_item_t *event_tail;
    send_item_t *send_head;
    send_item_t *send_tail;
    bool scheduled;
    bool closing;
    bool closed;
    bool write_interest;
    atomic_uint references;
    struct epoll_connection *ready_next;
    struct epoll_connection *all_next;
};

typedef struct {
    pthread_t *threads;
    size_t thread_count;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    epoll_connection_t *head;
    epoll_connection_t *tail;
    bool stopping;
} thread_pool_t;

struct epoll_server {
    int epoll_fd;
    int listen_fd;
    int wake_fd;
    size_t max_events;
    thread_pool_t pool;
    epoll_server_callbacks_t callbacks;
    void *user_data;
    atomic_bool stopping;
    pthread_mutex_t command_mutex;
    command_t *command_head;
    command_t *command_tail;
    epoll_connection_t *connections;
};

static char wake_tag;

static void connection_release(epoll_connection_t *connection);
static void request_command(epoll_connection_t *connection, command_type_t type);

static void connection_retain(epoll_connection_t *connection) {
    atomic_fetch_add_explicit(&connection->references, 1, memory_order_relaxed);
}

static void free_events(event_item_t *item) {
    while (item) {
        event_item_t *next = item->next;
        free(item);
        item = next;
    }
}

static void free_sends(send_item_t *item) {
    while (item) {
        send_item_t *next = item->next;
        free(item->data);
        free(item);
        item = next;
    }
}

static void connection_release(epoll_connection_t *connection) {
    if (atomic_fetch_sub_explicit(&connection->references, 1, memory_order_acq_rel) != 1)
        return;
    free_events(connection->event_head);
    free_sends(connection->send_head);
    pthread_mutex_destroy(&connection->mutex);
    free(connection);
}

static int pool_submit(thread_pool_t *pool, epoll_connection_t *connection) {
    connection_retain(connection);
    pthread_mutex_lock(&pool->mutex);
    if (pool->stopping) {
        pthread_mutex_unlock(&pool->mutex);
        connection_release(connection);
        return -1;
    }
    connection->ready_next = NULL;
    if (pool->tail)
        pool->tail->ready_next = connection;
    else
        pool->head = connection;
    pool->tail = connection;
    pthread_cond_signal(&pool->condition);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

static void notify_loop(epoll_server_t *server) {
    uint64_t one = 1;
    ssize_t ignored = write(server->wake_fd, &one, sizeof(one));
    (void)ignored;
}

static void request_command(epoll_connection_t *connection, command_type_t type) {
    epoll_server_t *server = connection->server;
    command_t *command = malloc(sizeof(*command));
    if (!command) {
        if (type == COMMAND_CLOSE)
            shutdown(connection->fd, SHUT_RDWR);
        return;
    }
    connection_retain(connection);
    command->type = type;
    command->connection = connection;
    command->next = NULL;
    pthread_mutex_lock(&server->command_mutex);
    if (server->command_tail)
        server->command_tail->next = command;
    else
        server->command_head = command;
    server->command_tail = command;
    pthread_mutex_unlock(&server->command_mutex);
    notify_loop(server);
}

int epoll_connection_send(epoll_connection_t *connection, const void *data, size_t size) {
    if (!connection || (!data && size) || size == 0)
        return size == 0 ? 0 : -1;
    send_item_t *item = calloc(1, sizeof(*item));
    if (!item)
        return -1;
    item->data = malloc(size);
    if (!item->data) {
        free(item);
        return -1;
    }
    memcpy(item->data, data, size);
    item->size = size;

    pthread_mutex_lock(&connection->mutex);
    if (connection->closing || connection->closed) {
        pthread_mutex_unlock(&connection->mutex);
        free(item->data);
        free(item);
        errno = EPIPE;
        return -1;
    }
    if (connection->send_tail)
        connection->send_tail->next = item;
    else
        connection->send_head = item;
    connection->send_tail = item;
    pthread_mutex_unlock(&connection->mutex);
    request_command(connection, COMMAND_REFRESH);
    return 0;
}

void epoll_connection_close(epoll_connection_t *connection) {
    if (!connection)
        return;
    pthread_mutex_lock(&connection->mutex);
    bool request = !connection->closing && !connection->closed;
    connection->closing = true;
    pthread_mutex_unlock(&connection->mutex);
    if (request)
        request_command(connection, COMMAND_CLOSE);
}

int epoll_connection_fd(const epoll_connection_t *connection) {
    return connection ? connection->fd : -1;
}

static bool flush_output(epoll_connection_t *connection) {
    for (;;) {
        pthread_mutex_lock(&connection->mutex);
        send_item_t *item = connection->send_head;
        if (!item) {
            pthread_mutex_unlock(&connection->mutex);
            request_command(connection, COMMAND_REFRESH);
            return true;
        }
        ssize_t written = send(connection->fd, item->data + item->offset,
                               item->size - item->offset, MSG_NOSIGNAL);
        if (written > 0) {
            item->offset += (size_t)written;
            if (item->offset == item->size) {
                connection->send_head = item->next;
                if (!connection->send_head)
                    connection->send_tail = NULL;
                free(item->data);
                free(item);
            }
            pthread_mutex_unlock(&connection->mutex);
            continue;
        }
        int error = errno;
        pthread_mutex_unlock(&connection->mutex);
        if (written < 0 && error == EINTR)
            continue;
        if (written < 0 && (error == EAGAIN || error == EWOULDBLOCK)) {
            request_command(connection, COMMAND_REFRESH);
            return true;
        }
        return false;
    }
}

static bool drain_input(epoll_connection_t *connection) {
    unsigned char buffer[16 * 1024];
    for (;;) {
        ssize_t count = recv(connection->fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            if (connection->server->callbacks.on_data)
                connection->server->callbacks.on_data(connection, buffer,
                                                       (size_t)count,
                                                       connection->server->user_data);
            continue;
        }
        if (count == 0)
            return false;
        if (errno == EINTR)
            continue;
        return errno == EAGAIN || errno == EWOULDBLOCK;
    }
}

static void process_connection(epoll_connection_t *connection) {
    for (;;) {
        pthread_mutex_lock(&connection->mutex);
        event_item_t *item = connection->event_head;
        if (!item) {
            /*
             * Clear scheduled only while holding the same mutex used by
             * enqueue_event().  An event appended before this check is handled
             * by this worker; an event appended after this check observes
             * scheduled == false and submits the connection again.
             */
            connection->scheduled = false;
            pthread_mutex_unlock(&connection->mutex);
            break;
        }
        connection->event_head = item->next;
        if (!connection->event_head)
            connection->event_tail = NULL;
        bool closed = connection->closed || connection->closing;
        pthread_mutex_unlock(&connection->mutex);

        uint32_t events = item->events;
        free(item);
        bool alive = !closed;
        if (alive && (events & EPOLLIN))
            alive = drain_input(connection);
        if (alive && (events & EPOLLOUT))
            alive = flush_output(connection);
        if (alive && (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)))
            alive = false;
        if (!alive)
            epoll_connection_close(connection);
    }
}

static void *worker_main(void *argument) {
    thread_pool_t *pool = argument;
    for (;;) {
        pthread_mutex_lock(&pool->mutex);
        while (!pool->head && !pool->stopping)
            pthread_cond_wait(&pool->condition, &pool->mutex);
        if (!pool->head && pool->stopping) {
            pthread_mutex_unlock(&pool->mutex);
            return NULL;
        }
        epoll_connection_t *connection = pool->head;
        pool->head = connection->ready_next;
        if (!pool->head)
            pool->tail = NULL;
        pthread_mutex_unlock(&pool->mutex);
        process_connection(connection);
        connection_release(connection);
    }
}

static int pool_init(thread_pool_t *pool, size_t count) {
    memset(pool, 0, sizeof(*pool));
    if (pthread_mutex_init(&pool->mutex, NULL))
        return -1;
    if (pthread_cond_init(&pool->condition, NULL)) {
        pthread_mutex_destroy(&pool->mutex);
        return -1;
    }
    pool->threads = calloc(count, sizeof(*pool->threads));
    if (!pool->threads) {
        pthread_cond_destroy(&pool->condition);
        pthread_mutex_destroy(&pool->mutex);
        return -1;
    }
    for (; pool->thread_count < count; ++pool->thread_count) {
        if (pthread_create(&pool->threads[pool->thread_count], NULL, worker_main, pool)) {
            pthread_mutex_lock(&pool->mutex);
            pool->stopping = true;
            pthread_cond_broadcast(&pool->condition);
            pthread_mutex_unlock(&pool->mutex);
            for (size_t i = 0; i < pool->thread_count; ++i)
                pthread_join(pool->threads[i], NULL);
            free(pool->threads);
            pool->threads = NULL;
            pthread_cond_destroy(&pool->condition);
            pthread_mutex_destroy(&pool->mutex);
            return -1;
        }
    }
    return 0;
}

static void pool_destroy(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->mutex);
    pool->stopping = true;
    pthread_cond_broadcast(&pool->condition);
    pthread_mutex_unlock(&pool->mutex);
    for (size_t i = 0; i < pool->thread_count; ++i)
        pthread_join(pool->threads[i], NULL);
    while (pool->head) {
        epoll_connection_t *next = pool->head->ready_next;
        connection_release(pool->head);
        pool->head = next;
    }
    free(pool->threads);
    pthread_cond_destroy(&pool->condition);
    pthread_mutex_destroy(&pool->mutex);
}

static int add_special_fd(epoll_server_t *server, int fd, void *pointer) {
    struct epoll_event event = {.events = EPOLLIN | EPOLLET};
    event.data.ptr = pointer;
    return epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, fd, &event);
}

static int create_listener(const epoll_server_config_t *config) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -1;
    int enabled = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    struct sockaddr_in address = {.sin_family = AF_INET,
                                  .sin_port = htons(config->port)};
    if (config->bind_address) {
        if (inet_pton(AF_INET, config->bind_address, &address.sin_addr) != 1) {
            close(fd);
            errno = EINVAL;
            return -1;
        }
    } else {
        address.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) ||
        listen(fd, config->backlog > 0 ? config->backlog : 128)) {
        close(fd);
        return -1;
    }
    return fd;
}

epoll_server_t *epoll_server_create(const epoll_server_config_t *config,
                                    const epoll_server_callbacks_t *callbacks,
                                    void *user_data) {
    if (!config || config->worker_count == 0) {
        errno = EINVAL;
        return NULL;
    }
    epoll_server_t *server = calloc(1, sizeof(*server));
    if (!server)
        return NULL;
    server->epoll_fd = server->listen_fd = server->wake_fd = -1;
    server->max_events = config->max_events ? config->max_events : 256;
    server->user_data = user_data;
    if (callbacks)
        server->callbacks = *callbacks;
    pthread_mutex_init(&server->command_mutex, NULL);
    atomic_init(&server->stopping, false);

    server->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    server->listen_fd = create_listener(config);
    server->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (server->epoll_fd < 0 || server->listen_fd < 0 || server->wake_fd < 0 ||
        add_special_fd(server, server->listen_fd, NULL) ||
        add_special_fd(server, server->wake_fd, &wake_tag) ||
        pool_init(&server->pool, config->worker_count)) {
        epoll_server_destroy(server);
        return NULL;
    }
    return server;
}

static epoll_connection_t *new_connection(epoll_server_t *server, int fd) {
    epoll_connection_t *connection = calloc(1, sizeof(*connection));
    if (!connection)
        return NULL;
    connection->fd = fd;
    connection->server = server;
    atomic_init(&connection->references, 1);
    pthread_mutex_init(&connection->mutex, NULL);
    connection->all_next = server->connections;
    server->connections = connection;
    return connection;
}

static void accept_connections(epoll_server_t *server) {
    for (;;) {
        int fd = accept4(server->listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            return;
        }
        epoll_connection_t *connection = new_connection(server, fd);
        struct epoll_event event = {.events = EPOLLIN | EPOLLRDHUP | EPOLLET};
        event.data.ptr = connection;
        if (!connection || epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, fd, &event)) {
            if (connection) {
                server->connections = connection->all_next;
                connection_release(connection);
            }
            close(fd);
            continue;
        }
        if (server->callbacks.on_open)
            server->callbacks.on_open(connection, server->user_data);
    }
}

static void enqueue_event(epoll_connection_t *connection, uint32_t events) {
    event_item_t *item = calloc(1, sizeof(*item));
    if (!item) {
        epoll_connection_close(connection);
        return;
    }
    pthread_mutex_lock(&connection->mutex);
    if (connection->closing || connection->closed) {
        pthread_mutex_unlock(&connection->mutex);
        free(item);
        return;
    }
    if (events & EPOLLIN) {
        for (event_item_t *prior = connection->event_head; prior; prior = prior->next) {
            if (prior->events & EPOLLIN) {
                events &= ~EPOLLIN;
                break;
            }
        }
    }
    if (!(events & CONNECTION_EVENT_MASK)) {
        pthread_mutex_unlock(&connection->mutex);
        free(item);
        return;
    }
    item->events = events;
    if (connection->event_tail)
        connection->event_tail->next = item;
    else
        connection->event_head = item;
    connection->event_tail = item;
    bool submit = !connection->scheduled;
    /* Set before unlocking so concurrent producers cannot submit this
     * connection to more than one worker. */
    connection->scheduled = true;
    pthread_mutex_unlock(&connection->mutex);

    if (!submit)
        return;

    int submit_result = pool_submit(&connection->server->pool, connection);
    if (submit_result != 0) {
        /* No worker owns this connection when submission fails.  This is the
         * only producer-side rollback; the normal path is cleared by the
         * worker after it observes an empty event queue. */
        pthread_mutex_lock(&connection->mutex);
        connection->scheduled = false;
        pthread_mutex_unlock(&connection->mutex);
        epoll_connection_close(connection);
    }
}

static void unlink_connection(epoll_server_t *server, epoll_connection_t *connection) {
    epoll_connection_t **cursor = &server->connections;
    while (*cursor && *cursor != connection)
        cursor = &(*cursor)->all_next;
    if (*cursor)
        *cursor = connection->all_next;
}

static void apply_command(epoll_server_t *server, command_t *command) {
    epoll_connection_t *connection = command->connection;
    if (command->type == COMMAND_CLOSE) {
        pthread_mutex_lock(&connection->mutex);
        bool do_close = !connection->closed;
        connection->closed = true;
        connection->closing = true;
        pthread_mutex_unlock(&connection->mutex);
        if (do_close) {
            epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, connection->fd, NULL);
            close(connection->fd);
            if (server->callbacks.on_close)
                server->callbacks.on_close(connection, server->user_data);
            unlink_connection(server, connection);
            connection_release(connection); /* event-loop ownership */
        }
    } else {
        pthread_mutex_lock(&connection->mutex);
        bool has_output = connection->send_head != NULL;
        bool usable = !connection->closing && !connection->closed;
        bool changed = connection->write_interest != has_output;
        connection->write_interest = has_output;
        pthread_mutex_unlock(&connection->mutex);
        if (usable && changed) {
            struct epoll_event event = {
                .events = EPOLLIN | EPOLLRDHUP | EPOLLET | (has_output ? EPOLLOUT : 0)};
            event.data.ptr = connection;
            if (epoll_ctl(server->epoll_fd, EPOLL_CTL_MOD, connection->fd, &event))
                epoll_connection_close(connection);
        }
    }
}

static void drain_commands(epoll_server_t *server) {
    uint64_t value;
    while (read(server->wake_fd, &value, sizeof(value)) > 0) {}
    pthread_mutex_lock(&server->command_mutex);
    command_t *commands = server->command_head;
    server->command_head = server->command_tail = NULL;
    pthread_mutex_unlock(&server->command_mutex);
    while (commands) {
        command_t *next = commands->next;
        apply_command(server, commands);
        connection_release(commands->connection); /* command ownership */
        free(commands);
        commands = next;
    }
}

int epoll_server_run(epoll_server_t *server) {
    if (!server) {
        errno = EINVAL;
        return -1;
    }
    struct epoll_event *events = calloc(server->max_events, sizeof(*events));
    if (!events)
        return -1;
    int result = 0;
    while (!atomic_load_explicit(&server->stopping, memory_order_acquire)) {
        int count = epoll_wait(server->epoll_fd, events, (int)server->max_events, -1);
        if (count < 0) {
            if (errno == EINTR)
                continue;
            result = -1;
            break;
        }
        for (int i = 0; i < count; ++i) {
            if (!events[i].data.ptr)
                accept_connections(server);
            else if (events[i].data.ptr == &wake_tag)
                drain_commands(server);
            else
                enqueue_event(events[i].data.ptr, events[i].events);
        }
    }
    drain_commands(server);
    free(events);
    return result;
}

void epoll_server_stop(epoll_server_t *server) {
    if (!server)
        return;
    atomic_store_explicit(&server->stopping, true, memory_order_release);
    notify_loop(server);
}

void epoll_server_destroy(epoll_server_t *server) {
    if (!server)
        return;
    epoll_server_stop(server);
    if (server->pool.threads)
        pool_destroy(&server->pool);
    drain_commands(server);
    while (server->connections) {
        epoll_connection_t *connection = server->connections;
        server->connections = connection->all_next;
        if (!connection->closed)
            close(connection->fd);
        connection->closed = true;
        connection_release(connection);
    }
    if (server->listen_fd >= 0)
        close(server->listen_fd);
    if (server->wake_fd >= 0)
        close(server->wake_fd);
    if (server->epoll_fd >= 0)
        close(server->epoll_fd);
    pthread_mutex_destroy(&server->command_mutex);
    free(server);
}
