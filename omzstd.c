#define _GNU_SOURCE
#include <unistd.h>
#include <time.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <fcntl.h>
#include <signal.h>
#include <zstd.h>
#include <pthread.h>

#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

#define LOG(...) _log_impl(__FILE__, __LINE__, __func__, __VA_ARGS__)

static pid_t myPid = 0;

static inline void _log_impl(const char *file, int line, const char *func, const char *fmt, ...)
{
    const time_t t = time(NULL);

    char buf[21];
    buf[strftime(buf, sizeof buf, "%FT%TZ", localtime(&t))] = '\0';

    fprintf(stderr, "%s %s@%s:%d[%d]: ", buf, func, file, line, myPid);

    va_list args;
    va_start(args, fmt);
// TODO: 'fmt' is not a string literal. this is bad!
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"

    vfprintf(stderr, fmt, args);

#pragma clang diagnostic pop

    va_end(args);
    fprintf(stderr, "\n");
}

static inline bool stdin_is_closed()
{
    return !(fcntl(STDIN_FILENO, F_GETFL) != -1 || errno != EBADF);
}

////////////////////////////////////////////////////////////////////////

typedef struct stream_t_
{
    size_t inputBufferSize;
    char *inputBuffer;

    size_t outputBufferSize;
    char *outputBuffer;

    ZSTD_CCtx *zctx;
    ZSTD_inBuffer zInBuf;
    ZSTD_outBuffer zOutBuf;

    FILE *outFile;
    const char *outFileName;
} stream_t;

static stream_t stream = {
    .inputBufferSize = 1024 * 1024 * 8,
    .inputBuffer = NULL,
    .outputBufferSize = 1024 * 1024 * 8,
    .outputBuffer = NULL,
    .zctx = NULL,
    .zInBuf = {
        .src = NULL,
        .size = 0,
        .pos = 0},
    .zOutBuf = {.dst = NULL, .size = 0, .pos = 0},
    .outFile = NULL,
    .outFileName = NULL};

static inline int flush_zstd()
{
    const ZSTD_EndDirective mode = ZSTD_e_end;
    ZSTD_inBuffer input = {"", 0, 0};

    size_t remaining = 0;
    do
    {
        remaining = ZSTD_compressStream2(stream.zctx, &stream.zOutBuf, &input, mode);
        if (ZSTD_isError(remaining))
        {
            LOG("error flushing ZSTD buffer: %s", ZSTD_getErrorName(remaining));
            return -1;
        }
        // TODO: add an upper limit of how often we try to flush
    } while (remaining != 0);

    if (stream.zOutBuf.pos != fwrite(stream.outputBuffer, 1, stream.zOutBuf.pos, stream.outFile))
    {
        LOG("error writing compressed buffer to file, exiting");
        return -1;
    }

    return 0;
}

static inline int reopen_file()
{
    if (stream.outFile != NULL)
    {
        const int fn = fileno(stream.outFile);
        if (fn == -1)
        {
            LOG("error getting file number for output file: %s", strerror(errno));
            return -1;
        }
        else
        {
            if (fsync(fn) == -1)
            {
                LOG("error syncing output file to disk: %s", strerror(errno));
                return -1;
            }
        }

        if (fclose(stream.outFile) != 0)
        {
            LOG("error closing output file: %s", strerror(errno));
            return -1;
        }

        stream.outFile = NULL;

        char outFileFullName[2048] = {0};
        snprintf(outFileFullName, 2048, "%s.%d.%lu", stream.outFileName, myPid, (unsigned long)time(NULL));

        stream.outFile = fopen(outFileFullName, "wb");
        if (stream.outFile == NULL)
        {
            LOG("error reopening file: %s", strerror(errno));
            return -1;
        }

        return 0;
    }

    return -1;
}

void handle_signal(int signum)
{
    if (signum == SIGHUP)
    {
        if (flush_zstd() != 0)
        {
            // we can't flush zstd, exiting
            LOG("can not flush ZSTD buffer, exiting");
            exit(1);
        }
        if (reopen_file() != 0)
        {
            // we can't reopen the file for writing, exiting
            LOG("can not reopen file, exiting");
            exit(1);
        }
    }
}

int main(int argc, char **argv)
{
    myPid = getpid();

    if (argc != 4)
    {
        LOG("usage: omzstd THREADS LEVEL PATH_PREFIX");
        exit(1);
    }

    const long workers = strtol(argv[1], NULL, 10);
    if (workers < 1)
    {
        LOG("invalid threads count");
        exit(1);
    }

    const long level = strtol(argv[2], NULL, 10);
    if (level < 1)
    {
        LOG("invalid compression level (1-19, default: 3)");
        exit(1);
    }

    stream.outFileName = argv[3];

    stream.inputBuffer = (char *)malloc(stream.inputBufferSize);
    if (stream.inputBuffer == NULL)
    {
        LOG("error allocating input buffer");
        exit(1);
    }

    stream.outputBuffer = (char *)malloc(stream.outputBufferSize);
    if (stream.outputBuffer == NULL)
    {
        LOG("error allocating output buffer");
        exit(1);
    }

    stream.zctx = ZSTD_createCCtx();
    if (stream.zctx == NULL)
    {
        LOG("error creating ZSTD context");
        exit(1);
    }

    {
        size_t const err = ZSTD_CCtx_setParameter(stream.zctx, ZSTD_c_compressionLevel, (int)(level));
        if (UNLIKELY(ZSTD_isError(err)))
        {
            LOG("error setting compression level %d: %s", level, ZSTD_getErrorName(err));
            exit(1);
        }
    }

    {
        size_t const err = ZSTD_CCtx_setParameter(stream.zctx, ZSTD_c_checksumFlag, 1);
        if (UNLIKELY(ZSTD_isError(err)))
        {
            LOG("error enabling ZSTD checksumming: %s", ZSTD_getErrorName(err));
            exit(1);
        }
    }

    if (workers != 1)
    {
        size_t const err = ZSTD_CCtx_setParameter(stream.zctx, ZSTD_c_nbWorkers, (int)(workers));
        if (UNLIKELY(ZSTD_isError(err)))
        {
            LOG("error settings threads to %d: %s", workers, ZSTD_getErrorName(err));
            exit(1);
        }
    }

    char outFileFullName[2048] = {0};
    snprintf(outFileFullName, 2048, "%s.%d.%lu", stream.outFileName, myPid, (unsigned long)time(NULL));

    stream.outFile = fopen(outFileFullName, "wb");
    if (stream.outFile == NULL)
    {
        LOG("error opening output file ('%s'): %s", stream.outFileName, strerror(errno));
        exit(1);
    }

    signal(SIGHUP, handle_signal);

    if (write(STDOUT_FILENO, "OK\n", 3) == -1)
    {
        LOG("error writing initial OK");
        exit(1);
    }

    while (1)
    {
        const ssize_t lret = getline(&stream.inputBuffer, &stream.inputBufferSize, stdin);
        if (UNLIKELY(lret < 1))
        {
            if (errno == 0)
            {
                LOG("stdin closed, exiting");
                goto flush;
            }
            LOG("error in getline: %d %d %s", lret, errno, strerror(errno));
            break;
        }

        stream.zInBuf.src = stream.inputBuffer;
        stream.zInBuf.size = (size_t)(lret);
        stream.zInBuf.pos = 0;

        stream.zOutBuf.dst = stream.outputBuffer;
        stream.zOutBuf.size = stream.outputBufferSize;
        stream.zOutBuf.pos = 0;

        size_t remaining = 0;
        do
        {
            remaining = ZSTD_compressStream2(stream.zctx, &stream.zOutBuf, &stream.zInBuf, ZSTD_e_continue);
            if (ZSTD_isError(remaining))
            {
                LOG("error flushing ZSTD buffer: %s", ZSTD_getErrorName(remaining));
                goto flush;
            }

            // debug
            // LOG("remaining: %d, pos: %d, size: %d", remaining, stream.zInBuf.pos, stream.zInBuf.size);

        } while (stream.zInBuf.pos != stream.zInBuf.size);

        if (UNLIKELY(stream.zOutBuf.pos > stream.outputBufferSize))
        {
            LOG("buffer overflow detected");
            goto flush;
        }

        //debug
        // LOG("OUT, pos: %d, size: %d", stream.zOutBuf.pos, stream.zOutBuf.size);

        if (stream.zOutBuf.pos != fwrite(stream.outputBuffer, 1, stream.zOutBuf.pos, stream.outFile))
        {
            LOG("error writing compressed buffer to file, exiting");
            goto flush;
        }

        if (write(STDOUT_FILENO, "OK\n", 3) == -1)
        {
            LOG("error writing initial OK");
            goto flush;
        }
    }

flush:
    if (flush_zstd() != 0)
    {
        LOG("can not flush ZSTD buffer");
    }
cleanup:

    if (stream.outFile != NULL)
    {
        const int fn = fileno(stream.outFile);
        if (fn == -1)
        {
            LOG("error getting file number for output file (%s): %s", stream.outFileName, strerror(errno));
        }
        else
        {
            if (fsync(fn) == -1)
            {
                LOG("error syncing output file to disk: %s", strerror(errno));
            }
        }

        if (fclose(stream.outFile) != 0)
        {
            LOG("error closing output file (%s): %s", stream.outFileName, strerror(errno));
        }

        stream.outFile = NULL;
    }

    if (stream.inputBuffer != NULL)
    {
        free(stream.inputBuffer);
        stream.inputBuffer = NULL;
        stream.inputBufferSize = 0;
    }

    if (stream.outputBuffer != NULL)
    {
        free(stream.outputBuffer);
        stream.outputBuffer = NULL;
        stream.outputBufferSize = 0;
    }

    return 0;
}