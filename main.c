#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <dirent.h>
#include <time.h>

#define MAX_PATTERN  256
#define MAX_PATH     4096
#define MAX_FILES    100000
#define NUM_THREADS  4
#define QUEUE_CAP    512
#define SMALL_FILE   1048576

char g_pattern[MAX_PATTERN];
int  g_case_sensitive;
char g_root[MAX_PATH];
int  g_mode;

size_t g_files_scanned;
size_t g_matches_found;
size_t g_dirs_visited;
size_t g_errors;

pthread_mutex_t g_stats_mtx;
pthread_mutex_t g_out_mtx;
pthread_mutex_t g_file_mtx;
pthread_mutex_t g_q_mtx;

char*  g_file_list[MAX_FILES];
size_t g_file_count;

size_t g_bad_char[256];
char   g_pat_lower[MAX_PATTERN];
size_t g_pat_len;

typedef struct {
    char* path;
} Task;

Task g_queue[QUEUE_CAP];
int  g_q_head;
int  g_q_tail;
int  g_q_count;
int  g_done;

sem_t g_task_sem;
sem_t g_slot_sem;

typedef struct {
    const char* data;
    size_t      start;
    size_t      end;
    size_t      base_line;
    char        filepath[MAX_PATH];
} SpanArg;

typedef struct {
    size_t start;
    size_t end;
} RangeArg;


char lower_c(char c) {
    if (c >= 'A' && c <= 'Z') return c + 32;
    return c;
}

void build_table() {
    int i;
    g_pat_len = strlen(g_pattern);
    for (i = 0; i < 256; i++) g_bad_char[i] = g_pat_len;

    if (!g_case_sensitive) {
        for (i = 0; i < (int)g_pat_len; i++)
            g_pat_lower[i] = lower_c(g_pattern[i]);
        g_pat_lower[g_pat_len] = '\0';
    }

    for (i = 0; i < (int)g_pat_len - 1; i++) {
        unsigned char c = (unsigned char)g_pattern[i];
        g_bad_char[c] = g_pat_len - 1 - i;
        if (!g_case_sensitive) {
            g_bad_char[(unsigned char)lower_c(c)] = g_pat_len - 1 - i;
        }
    }
}

int search_in(const char* text, size_t n) {
    size_t i, k, j;
    const char* pat;
    char tc;

    if (g_pat_len == 0) return 1;
    if (n < g_pat_len)  return 0;

    pat = g_case_sensitive ? g_pattern : g_pat_lower;
    i   = g_pat_len - 1;

    while (i < n) {
        k = i;
        j = g_pat_len - 1;
        while (1) {
            tc = g_case_sensitive ? text[k] : lower_c(text[k]);
            if (tc != pat[j]) break;
            if (j == 0) return 1;
            k--;
            j--;
        }
        i += g_bad_char[(unsigned char)text[i]];
    }
    return 0;
}

void print_match(const char* path, size_t line_no, const char* line, size_t len) {
    pthread_mutex_lock(&g_out_mtx);
    printf("\033[1;36m%s\033[0m:\033[1;33m%zu\033[0m: %.*s\n", path, line_no, (int)len, line);
    pthread_mutex_unlock(&g_out_mtx);
}

void print_file_hit(const char* path) {
    pthread_mutex_lock(&g_out_mtx);
    printf("\033[1;32mFOUND\033[0m \033[1m%s\033[0m\n", path);
    pthread_mutex_unlock(&g_out_mtx);
}

void print_warn(const char* msg) {
    pthread_mutex_lock(&g_out_mtx);
    fprintf(stderr, "\033[1;31mWARN:\033[0m %s\n", msg);
    pthread_mutex_unlock(&g_out_mtx);
}

void stats_add(size_t* stat, size_t n) {
    pthread_mutex_lock(&g_stats_mtx);
    *stat += n;
    pthread_mutex_unlock(&g_stats_mtx);
}

void* span_worker(void* raw) {
    SpanArg*    a      = (SpanArg*)raw;
    const char* ptr    = a->data + a->start;
    const char* end    = a->data + a->end;
    size_t      line_no = a->base_line;
    size_t      hits   = 0;
    const char* nl;
    const char* le;
    size_t      len;

    while (ptr < end) {
        nl  = (const char*)memchr(ptr, '\n', (size_t)(end - ptr));
        le  = nl ? nl : end;
        len = (size_t)(le - ptr);

        if (search_in(ptr, len)) {
            hits++;
            print_match(a->filepath, line_no, ptr, len);
        }

        line_no++;
        ptr = nl ? nl + 1 : end;
    }

    if (hits > 0) stats_add(&g_matches_found, hits);
    free(a);
    return NULL;
}

void search_text_file(const char* path) {
    int         fd;
    struct stat st;
    size_t      size;
    char*       data;
    int         i, actual;
    size_t      chunk;
    size_t      pos;
    size_t      starts[NUM_THREADS + 1];
    size_t      base_lines[NUM_THREADS + 1];
    pthread_t   tids[NUM_THREADS];
    SpanArg*    a;
    size_t      lines, j;

    stats_add(&g_files_scanned, 1);

    fd = open(path, O_RDONLY);
    if (fd < 0) { print_warn(path); stats_add(&g_errors, 1); return; }

    if (fstat(fd, &st) < 0) { close(fd); return; }
    size = (size_t)st.st_size;
    if (size == 0) { close(fd); return; }

    data = (char*)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (data == MAP_FAILED) { stats_add(&g_errors, 1); return; }
    madvise(data, size, MADV_SEQUENTIAL);

    if (size < SMALL_FILE) {
        a = (SpanArg*)malloc(sizeof(SpanArg));
        a->data      = data;
        a->start     = 0;
        a->end       = size;
        a->base_line = 1;
        strncpy(a->filepath, path, MAX_PATH - 1);
        span_worker(a);
        munmap(data, size);
        return;
    }

    chunk       = size / NUM_THREADS;
    starts[0]   = 0;
    base_lines[0] = 1;
    actual      = 1;

    for (i = 1; i < NUM_THREADS; i++) {
        pos = i * chunk;
        while (pos < size && data[pos] != '\n') pos++;
        if (pos < size) pos++;
        if (pos != starts[actual - 1] && pos < size) {
            starts[actual] = pos;
            actual++;
        }
    }
    starts[actual] = size;

    for (i = 1; i < actual; i++) {
        lines = base_lines[i - 1];
        for (j = starts[i - 1]; j < starts[i]; j++)
            if (data[j] == '\n') lines++;
        base_lines[i] = lines;
    }

    for (i = 0; i < actual; i++) {
        a = (SpanArg*)malloc(sizeof(SpanArg));
        a->data      = data;
        a->start     = starts[i];
        a->end       = starts[i + 1];
        a->base_line = base_lines[i];
        strncpy(a->filepath, path, MAX_PATH - 1);
        pthread_create(&tids[i], NULL, span_worker, a);
    }
    for (i = 0; i < actual; i++)
        pthread_join(tids[i], NULL);

    munmap(data, size);
}

void* text_pool_worker(void* arg) {
    char* path;
    (void)arg;

    for (;;) {
        sem_wait(&g_task_sem);

        pthread_mutex_lock(&g_q_mtx);
        if (g_done && g_q_count == 0) {
            pthread_mutex_unlock(&g_q_mtx);
            sem_post(&g_task_sem);
            break;
        }

        path = NULL;
        if (g_q_count > 0) {
            path    = g_queue[g_q_head].path;
            g_q_head = (g_q_head + 1) % QUEUE_CAP;
            g_q_count--;
        }
        pthread_mutex_unlock(&g_q_mtx);
        sem_post(&g_slot_sem);

        if (path) {
            search_text_file(path);
            free(path);
        }
    }
    return NULL;
}

void crawl(const char* path) {
    DIR*           dir;
    struct dirent* entry;
    char           full[MAX_PATH];

    dir = opendir(path);
    if (!dir) return;

    stats_add(&g_dirs_visited, 1);

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        snprintf(full, MAX_PATH, "%s/%s", path, entry->d_name);

        if (entry->d_type == DT_DIR) {
            crawl(full);
        } else if (entry->d_type == DT_REG) {
            if (g_mode == 1) {
                pthread_mutex_lock(&g_file_mtx);
                if (g_file_count < MAX_FILES)
                    g_file_list[g_file_count++] = strdup(full);
                pthread_mutex_unlock(&g_file_mtx);
            } else {
                sem_wait(&g_slot_sem);
                pthread_mutex_lock(&g_q_mtx);
                g_queue[g_q_tail].path = strdup(full);
                g_q_tail  = (g_q_tail + 1) % QUEUE_CAP;
                g_q_count++;
                pthread_mutex_unlock(&g_q_mtx);
                sem_post(&g_task_sem);
            }
        }
    }
    closedir(dir);
}

void* file_match_worker(void* raw) {
    RangeArg*   r     = (RangeArg*)raw;
    size_t      i;
    const char* full;
    const char* slash;
    const char* fname;
    size_t      fn_len;
    size_t      pt_len;
    int         match;
    size_t      hits;
    size_t      j;

    hits  = 0;
    pt_len = strlen(g_pattern);

    for (i = r->start; i < r->end; i++) {
        full  = g_file_list[i];
        slash = strrchr(full, '/');
        fname = slash ? slash + 1 : full;
        fn_len = strlen(fname);

        match = 0;
        if (fn_len == pt_len) {
            match = 1;
            for (j = 0; j < fn_len; j++) {
                char a = g_case_sensitive ? fname[j]      : lower_c(fname[j]);
                char b = g_case_sensitive ? g_pattern[j]  : lower_c(g_pattern[j]);
                if (a != b) { match = 0; break; }
            }
        }

        if (match) { hits++; print_file_hit(full); }
    }

    if (hits > 0) stats_add(&g_matches_found, hits);
    stats_add(&g_files_scanned, r->end - r->start);
    free(r);
    return NULL;
}

int main(int argc, char** argv) {
    int            i;
    struct timespec t0, t1;
    double         ms;
    pthread_t      workers[NUM_THREADS];
    size_t         n, stride, start, end;
    int            launched;
    RangeArg*      range;

    if (argc < 3) {
        fprintf(stderr, "Usage: ./search <text|file> <pattern> [root] [-c]\n");
        return 1;
    }

    if (strcmp(argv[1], "text") == 0)      g_mode = 0;
    else if (strcmp(argv[1], "file") == 0) g_mode = 1;
    else { fprintf(stderr, "mode must be text or file\n"); return 1; }

    strncpy(g_pattern, argv[2], MAX_PATTERN - 1);

    if (argc >= 4 && strcmp(argv[3], "null") != 0)
        strncpy(g_root, argv[3], MAX_PATH - 1);
    else
        getcwd(g_root, MAX_PATH);

    for (i = 4; i < argc; i++)
        if (strcmp(argv[i], "-c") == 0) g_case_sensitive = 1;

    pthread_mutex_init(&g_stats_mtx, NULL);
    pthread_mutex_init(&g_out_mtx,   NULL);
    pthread_mutex_init(&g_file_mtx,  NULL);
    pthread_mutex_init(&g_q_mtx,     NULL);

    g_files_scanned = 0;
    g_matches_found = 0;
    g_dirs_visited  = 0;
    g_errors        = 0;
    g_file_count    = 0;
    g_q_head        = 0;
    g_q_tail        = 0;
    g_q_count       = 0;
    g_done          = 0;

    build_table();

    clock_gettime(CLOCK_MONOTONIC, &t0);

    if (g_mode == 0) {
        sem_init(&g_task_sem, 0, 0);
        sem_init(&g_slot_sem, 0, QUEUE_CAP);

        for (i = 0; i < NUM_THREADS; i++)
            pthread_create(&workers[i], NULL, text_pool_worker, NULL);

        crawl(g_root);

        pthread_mutex_lock(&g_q_mtx);
        g_done = 1;
        pthread_mutex_unlock(&g_q_mtx);
        sem_post(&g_task_sem);

        for (i = 0; i < NUM_THREADS; i++)
            pthread_join(workers[i], NULL);

        sem_destroy(&g_task_sem);
        sem_destroy(&g_slot_sem);

    } else {
        crawl(g_root);

        n        = g_file_count;
        stride   = n / NUM_THREADS;
        if (stride == 0) stride = 1;
        launched = 0;

        for (i = 0; i < NUM_THREADS; i++) {
            start = i * stride;
            end   = (i + 1 == NUM_THREADS) ? n : start + stride;
            if (start >= n) break;

            range        = (RangeArg*)malloc(sizeof(RangeArg));
            range->start = start;
            range->end   = end;
            pthread_create(&workers[launched++], NULL, file_match_worker, range);
        }

        for (i = 0; i < launched; i++)
            pthread_join(workers[i], NULL);

        for (i = 0; i < (int)g_file_count; i++)
            free(g_file_list[i]);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    printf("\n--- Summary ---\n");
    printf("Dirs    : %zu\n", g_dirs_visited);
    printf("Scanned : %zu files\n", g_files_scanned);
    printf("Matches : %zu\n", g_matches_found);
    printf("Errors  : %zu\n", g_errors);
    printf("Time    : %.2f ms\n", ms);

    pthread_mutex_destroy(&g_stats_mtx);
    pthread_mutex_destroy(&g_out_mtx);
    pthread_mutex_destroy(&g_file_mtx);
    pthread_mutex_destroy(&g_q_mtx);

    return g_matches_found > 0 ? 0 : 2;
}