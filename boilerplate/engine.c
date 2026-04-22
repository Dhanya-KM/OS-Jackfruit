/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 * - command-line shape is defined
 * - key runtime data structures are defined
 * - bounded-buffer skeleton is defined
 * - supervisor / client split is outlined
 *
 * Students are expected to design:
 * - the control-plane IPC implementation
 * - container lifecycle and metadata synchronization
 * - clone + namespace setup for each container
 * - producer/consumer behavior for log buffering
 * - signal handling and graceful shutdown
 * * -----------------------------------------------------------
 * DESIGN & IMPLEMENTATION BY:
 * - Dhanya K.M. (Container Lifecycle, Clone/Namespaces, IPC)
 * - Nidhi R. (Bounded-Buffer logic, Logging Thread, CLI integration)
 * -----------------------------------------------------------
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 2048
#define CHILD_COMMAND_LEN 2048
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int log_read_fd;   /* read end of the pipe for this container's output */
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* -------------------------------------------------------------------
 * log_pump_arg: carries context for per-container log pump threads.
 * Each container gets its own thread that reads from the pipe read-end
 * and pushes chunks into the shared bounded buffer.
 * ------------------------------------------------------------------- */
typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} log_pump_arg_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0) return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0) return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            req->nice_value = atoi(argv[i + 1]);
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING: return "running";
    case CONTAINER_STOPPED: return "stopped";
    case CONTAINER_KILLED: return "killed";
    case CONTAINER_EXITED: return "exited";
    default: return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    memset(buffer, 0, sizeof(*buffer));
    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->not_empty, NULL);
    pthread_cond_init(&buffer->not_full, NULL);
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 * - block or fail according to your chosen policy when the buffer is full
 * - wake consumers correctly
 * - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 * - wait correctly while the buffer is empty
 * - return a useful status when shutdown is in progress
 * - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 * - remove log chunks from the bounded buffer
 * - route each chunk to the correct per-container log file
 * - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            if (write(fd, item.data, item.length) < 0) { perror("log write"); }
            close(fd);
        }
    }
    return NULL;
}

/* -------------------------------------------------------------------
 * log_pump_thread: per-container producer thread.
 *
 * Reads from the container's stdout/stderr pipe in chunks and pushes
 * each chunk into the shared bounded buffer for the logging thread to
 * persist. Exits when the pipe read-end returns EOF (container exited).
 * ------------------------------------------------------------------- */
static void *log_pump_thread(void *arg)
{
    log_pump_arg_t *pump = (log_pump_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    snprintf(item.container_id, sizeof(item.container_id), "%s", pump->container_id);

    while ((n = read(pump->read_fd, item.data, LOG_CHUNK_SIZE)) > 0) {
        item.length = (size_t)n;
        bounded_buffer_push(&pump->ctx->log_buffer, &item);
        memset(item.data, 0, LOG_CHUNK_SIZE);
    }

    close(pump->read_fd);
    free(pump);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 * - isolated PID / UTS / mount context
 * - chroot or pivot_root into rootfs
 * - working /proc inside container
 * - stdout / stderr redirected to the supervisor logging path
 * - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;
    setpriority(PRIO_PROCESS, 0, config->nice_value);

    if (chroot(config->rootfs) != 0) { perror("chroot"); exit(1); }
    if (chdir("/") != 0) { perror("chdir"); exit(1); }
    mount("proc", "/proc", "proc", 0, NULL);

    dup2(config->log_write_fd, STDOUT_FILENO);
    dup2(config->log_write_fd, STDERR_FILENO);
    close(config->log_write_fd);

    char *argv[] = {"/bin/sh", "-c", config->command, NULL};
    execvp("/bin/sh", argv);
    perror("execvp");
    return 1;
}

int register_with_monitor(int monitor_fd, const char *container_id, pid_t host_pid,
                          unsigned long soft_limit_bytes, unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    return ioctl(monitor_fd, MONITOR_REGISTER, &req);
}

/* -------------------------------------------------------------------
 * unregister_with_monitor: tell the kernel module to stop tracking
 * a container process when it is stopped or reaped.
 * ------------------------------------------------------------------- */
static int unregister_with_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    return ioctl(monitor_fd, MONITOR_UNREGISTER, &req);
}

/* -------------------------------------------------------------------
 * reap_children: non-blocking waitpid loop called from the supervisor
 * accept loop. Updates container_record state for any child that has
 * exited or been signalled since the last call.
 * ------------------------------------------------------------------- */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *curr = ctx->containers;
        while (curr) {
            if (curr->host_pid == pid) {
                if (WIFEXITED(status)) {
                    curr->state = CONTAINER_EXITED;
                    curr->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    curr->state = CONTAINER_KILLED;
                    curr->exit_signal = WTERMSIG(status);
                }
                /* Unregister from the kernel monitor now that the process is gone */
                if (ctx->monitor_fd >= 0)
                    unregister_with_monitor(ctx->monitor_fd, curr->id, pid);
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* -------------------------------------------------------------------
 * spawn_container: creates the actual container process with clone(),
 * sets up the log pipe, starts a log pump thread, registers the PID
 * with the kernel monitor, and fills in the container_record.
 *
 * Returns 0 on success, -1 on failure.
 * ------------------------------------------------------------------- */
static int spawn_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           container_record_t *rec)
{
    int log_fds[2];
    if (pipe(log_fds) < 0) {
        perror("pipe");
        return -1;
    }

    /* child_config lives on the heap so the child can safely access it
     * after clone() returns in the parent. The child copies what it needs
     * before exec so this is fine even without synchronisation. */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) { close(log_fds[0]); close(log_fds[1]); return -1; }

    snprintf(cfg->id,      sizeof(cfg->id),      "%s", req->container_id);
    snprintf(cfg->rootfs,  sizeof(cfg->rootfs),  "%s", req->rootfs);
    snprintf(cfg->command, sizeof(cfg->command), "%s", req->command);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = log_fds[1];

    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); close(log_fds[0]); close(log_fds[1]); return -1; }

    pid_t pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD,
                      cfg);

    /* Parent closes the write end; only the child should write to it */
    close(log_fds[1]);
    free(stack);

    if (pid < 0) {
        perror("clone");
        free(cfg);
        close(log_fds[0]);
        return -1;
    }

    free(cfg);  /* child has already exec'd or exited by this point */

    /* Fill in the record */
    rec->host_pid          = pid;
    rec->started_at        = time(NULL);
    rec->state             = CONTAINER_RUNNING;
    rec->soft_limit_bytes  = req->soft_limit_bytes;
    rec->hard_limit_bytes  = req->hard_limit_bytes;
    rec->log_read_fd       = log_fds[0];
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, rec->id);

    /* Start a pump thread that forwards the container's output into the
     * bounded buffer so the logging thread can persist it. */
    log_pump_arg_t *pump = malloc(sizeof(log_pump_arg_t));
    if (pump) {
        pump->ctx     = ctx;
        pump->read_fd = log_fds[0];
        snprintf(pump->container_id, sizeof(pump->container_id), "%s", rec->id);
        pthread_t pump_tid;
        pthread_create(&pump_tid, NULL, log_pump_thread, pump);
        pthread_detach(pump_tid);
    }

    /* Register with the kernel memory monitor if it is loaded */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 * - create and bind the control-plane IPC endpoint
 * - initialize shared metadata and the bounded buffer
 * - start the logging thread
 * - accept control requests and update container state
 * - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    mkdir(LOG_DIR, 0755);
    bounded_buffer_init(&ctx.log_buffer);
    pthread_mutex_init(&ctx.metadata_lock, NULL); /* ಲೋಡ್ ಮೆಟಾಡೇಟಾ ಲಾಕ್ */
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    /* It is normal for monitor_fd to be -1 when the kernel module is not loaded */

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);
    unlink(CONTROL_PATH);
    bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(ctx.server_fd, 5);
    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
setsockopt(ctx.server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    printf("[supervisor] Rootfs: %s. Socket: %s\n", rootfs, CONTROL_PATH);

    while (!ctx.should_stop) {
        /* Reap any children that have exited since the last iteration */
        reap_children(&ctx);

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        control_request_t req;
        if (recv(client_fd, &req, sizeof(req), 0) > 0) {
            control_response_t res;
            memset(&res, 0, sizeof(res));
            res.status = 0;
            snprintf(res.message, sizeof(res.message), "Success");

            /* ----------------------------------------------------------
             * CMD_START / CMD_RUN
             * Both commands spawn a new container. CMD_RUN is identical
             * to CMD_START in this implementation (both are fire-and-forget
             * from the client's perspective).
             * ---------------------------------------------------------- */
            if (req.kind == CMD_START || req.kind == CMD_RUN) {
                /* ಕಂಟೇನರ್ ರೆಕಾರ್ಡ್ ಕ್ರಿಯೇಟ್ ಮಾಡಿ ಲಿಸ್ಟ್‌ಗೆ ಸೇರಿಸುವ ಲಾಜಿಕ್ ಇಲ್ಲಿ ಬರಬೇಕು */
                container_record_t *new_rec = calloc(1, sizeof(container_record_t));
                if (!new_rec) {
                    res.status = -1;
                    snprintf(res.message, sizeof(res.message), "Out of memory");
                    send(client_fd, &res, sizeof(res), 0);
                    close(client_fd);
                    continue;
                }
                snprintf(new_rec->id, sizeof(new_rec->id), "%s", req.container_id);
                new_rec->state = CONTAINER_STARTING;

                if (spawn_container(&ctx, &req, new_rec) < 0) {
                    free(new_rec);
                    res.status = -1;
                    snprintf(res.message, sizeof(res.message), "Failed to spawn container");
                } else {
                    pthread_mutex_lock(&ctx.metadata_lock);
                    new_rec->next  = ctx.containers;
                    ctx.containers = new_rec;
                    pthread_mutex_unlock(&ctx.metadata_lock);
                    snprintf(res.message, CONTROL_MESSAGE_LEN,
                             "Container %s started (pid %d)", req.container_id, new_rec->host_pid);
                }
            }

            /* ----------------------------------------------------------
             * CMD_PS
             * Serialize the container table into res.message so the
             * client can print it without needing access to the supervisor
             * terminal.
             * ---------------------------------------------------------- */
            else if (req.kind == CMD_PS) {
                pthread_mutex_lock(&ctx.metadata_lock);
                /* Reap once more before listing so states are fresh */
                reap_children(&ctx);

                char table[CONTROL_MESSAGE_LEN];
                int offset = 0;
                offset += snprintf(table + offset, sizeof(table) - offset,
                                   "%-12s %-8s %-10s\n", "ID", "PID", "STATUS");

                container_record_t *curr = ctx.containers;
                while (curr && offset < (int)sizeof(table) - 1) {
                    offset += snprintf(table + offset, sizeof(table) - offset,
                                       "%-12s %-8d %-10s\n",
                                       curr->id, curr->host_pid, state_to_string(curr->state));
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                snprintf(res.message, sizeof(res.message), "%s", table);
            }

            /* ----------------------------------------------------------
             * CMD_LOGS
             * Read the container's log file and stream its content back
             * in the response message (truncated to CONTROL_MESSAGE_LEN).
             * For large logs the client can open the file directly.
             * ---------------------------------------------------------- */
            else if (req.kind == CMD_LOGS) {
                char log_path[PATH_MAX];
                snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req.container_id);
                int lfd = open(log_path, O_RDONLY);
                if (lfd < 0) {
                    res.status = -1;
                    snprintf(res.message, CONTROL_MESSAGE_LEN,
                             "No log found for container: %s", req.container_id);
                } else {
                    ssize_t n = read(lfd, res.message, CONTROL_MESSAGE_LEN - 1);
                    if (n < 0) n = 0;
                    res.message[n] = '\0';
                    close(lfd);
                }
            }

            /* ----------------------------------------------------------
             * CMD_STOP
             * Send SIGTERM to the container process and update its state.
             * The child will be reaped by the next reap_children() call.
             * ---------------------------------------------------------- */
            else if (req.kind == CMD_STOP) {
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                int found = 0;
                while (curr) {
                    if (strncmp(curr->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                        if (curr->state == CONTAINER_RUNNING) {
                            kill(curr->host_pid, SIGTERM);
                            curr->state = CONTAINER_STOPPED;
                        }
                        snprintf(res.message, CONTROL_MESSAGE_LEN,
                                 "Stopped container %s (pid %d)", curr->id, curr->host_pid);
                        found = 1;
                        break;
                    }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);

                if (!found) {
                    res.status = -1;
                    snprintf(res.message, CONTROL_MESSAGE_LEN,
                             "Container not found: %s", req.container_id);
                }
            }

            send(client_fd, &res, sizeof(res), 0);
        }
        close(client_fd);
    }

    /* ------------------------------------------------------------------
     * Cleanup: signal all running containers, wait for them, then tear
     * down threads and synchronization primitives.
     * ------------------------------------------------------------------ */

    /* SIGTERM all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *curr = ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING)
            kill(curr->host_pid, SIGTERM);
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait for them all to exit */
    reap_children(&ctx);

    /* Free the container record linked list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *node = ctx.containers;
    while (node) {
        container_record_t *next = node->next;
        free(node);
        node = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    pthread_mutex_destroy(&ctx.metadata_lock);
    bounded_buffer_destroy(&ctx.log_buffer);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option.
 */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Connect failed");
        close(fd);
        return 1;
    }

    send(fd, req, sizeof(*req), 0);

    control_response_t res;
    memset(&res, 0, sizeof(res));

    if (recv(fd, &res, sizeof(res), 0) > 0) {
        printf("%s\n", res.message);
    }

    close(fd);
    return res.status;
}

/* CLI Handlers (Remaining code stays as provided) */
static int cmd_start(int argc, char *argv[]) {
    control_request_t req = { .kind = CMD_START };
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    snprintf(req.rootfs,       sizeof(req.rootfs),       "%s", argv[3]);
    snprintf(req.command,      sizeof(req.command),      "%s", argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    parse_optional_flags(&req, argc, argv, 5);
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[]) {
    control_request_t req = { .kind = CMD_RUN };
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    snprintf(req.rootfs,       sizeof(req.rootfs),       "%s", argv[3]);
    snprintf(req.command,      sizeof(req.command),      "%s", argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    parse_optional_flags(&req, argc, argv, 5);
    return send_control_request(&req);
}

/* FIX: cmd_ps now reuses send_control_request instead of duplicating
 * the socket logic. The container table is printed from res.message,
 * which the supervisor populates on the server side. */
static int cmd_ps(void) {
    control_request_t req = { .kind = CMD_PS };
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[]) {
    (void)argc;
    control_request_t req = { .kind = CMD_LOGS };
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[]) {
    (void)argc;
    control_request_t req = { .kind = CMD_STOP };
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }
    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) { usage(argv[0]); return 1; }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run") == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps") == 0) return cmd_ps();
    if (strcmp(argv[1], "logs") == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop") == 0) return cmd_stop(argc, argv);
    usage(argv[0]);
    return 1;
}
