#include <stdlib.h>
#include <stdio.h>
#include <Windows.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <assert.h>
#include <process.h>

#include "arcci.h"

#define MAX_ARC_T 1024
typedef struct worker_arg
{
    arc_t *arc;
    int tid;
    HANDLE thr;
};

/* -------------- */
/* LOCAL VARIABLE */
/* -------------- */
static volatile int ok_to_run = 0;
static volatile int global_tick = 0;
#define BUF_LARGE_SIZE	10*1024
static char *buf_large = NULL;
CRITICAL_SECTION global_lock;

/* --------------- */
/* LOCAL FUNCTIONS */
/* --------------- */
unsigned int __stdcall
worker_thread(void *data)
{
    int ret;
    struct worker_arg *arg = (struct worker_arg *) data;
    long long count, saved_count;
    long long error, saved_error;
    int local_tick;
    int i;

    assert(arg != NULL);

    local_tick = global_tick;
    saved_count = count = 0;
    saved_error = error = 0;

    while (ok_to_run)
    {
        arc_request_t *rqst;
        arc_reply_t *reply;
        int rand_val = rand();
        int next_rqst = 0;
        int be_errno;
        const int PIPE_MAX = 50;

    peek_rqst:
        rqst = arc_create_request();
        assert(rqst != NULL);
        switch (next_rqst)
        {
        case 0:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret =
                    arc_append_command(rqst, "SET %s%d %s", "key", rand_val, "val");
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 1:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret = arc_append_command(rqst, "GET %s%d", "key", rand_val);
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 2:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret = arc_append_command(rqst, "DEL %s%d", "key", rand_val);
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 3:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret =
                    arc_append_command(rqst, "SET %s%d %s", "key", rand_val + 1, "val");
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 4:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret =
                    arc_append_command(rqst, "MGET %s%d %s%d", "key", rand_val,
                    "key", rand_val + 1);
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 5:
            ret =
            	arc_append_command(rqst, "SET %s%d %s", "key_large", rand_val + 1,
            	buf_large);
            assert(ret == 0);
            next_rqst++;
        case 6:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret =
                    arc_append_command(rqst, "SET %s%d %s", "key", rand_val + 1,
                    "val");
                assert(ret == 0);
            }
            next_rqst++;
            break;
        case 7:
            for (i = 0; i < PIPE_MAX; i++)
            {
                ret =
                    arc_append_command(rqst, "MGET %s%d %s%d", "key", rand_val,
                    "key", rand_val + 1);
                assert(ret == 0);
            }
            next_rqst++;
            /* fall thourgh */
        default:
            next_rqst = 0;
            break;
        }

        ret = arc_do_request(arg->arc, rqst, 3000, &be_errno);
        if (ret == 0)
        {
            count++;
            while ((ret = arc_get_reply(rqst, &reply, &be_errno)) == 0
                && reply != NULL)
            {
                if (reply == NULL)
                {
                    break;
                }
                arc_free_reply(reply);
                reply = NULL;
            }
            assert(ret == 0);
        }
        else
        {
            reply = NULL;
            ret = arc_get_reply(rqst, &reply, &be_errno);
            assert(ret == -1);
            assert(reply == NULL);
            error++;
        }
        arc_free_request(rqst);

        /* print stats */
        EnterCriticalSection(&global_lock);
        if (local_tick != global_tick)
        {
            printf("[%d][%d] %lld %lld\n", global_tick, arg->tid,
                count - saved_count, error - saved_error);
            local_tick = global_tick;
            saved_count = count;
            saved_error = error;
        }
        LeaveCriticalSection(&global_lock);

        if (next_rqst != 0)
        {
            goto peek_rqst;
        }
    }

    return 0;
}

static void
arg_error(char *msg)
{
    printf("%s\n", msg);
    exit(1);
}

/* -------------- */
/* Multi Instance */
/* -------------- */
int
dummy_perf_multi_instance(char *zk_addr, char *cluster_name, int num_thr, int run_sec)
{
    arc_t *arc[MAX_ARC_T];
    char *hosts = NULL;
    int remain_sec;
    struct worker_arg *args = NULL;
    int i;
    arc_conf_t conf;
    int log_level = ARC_LOG_LEVEL_INFO;

    arc_init_conf(&conf);
    conf.log_level = (arc_log_level_t)log_level;
    conf.log_file_prefix = "arcci";
    conf.zk_reconnect_millis = 3000;
    conf.init_timeout_millis = 5000;
    conf.conn_reconnect_millis = 5000;
    conf.num_conn_per_gw = 2;
    conf.max_fd = 0x7FFF;

    args = (struct worker_arg*)malloc(sizeof (struct worker_arg) * num_thr);
    assert(args != NULL);

    ok_to_run = 1;

    /* launch workers */
    for (i = 0; i < num_thr; i++)
    {
        arc[i] = arc_new_zk(zk_addr, cluster_name, &conf);
        assert(arc[i] != NULL);
    }

    for (i = 0; i < num_thr; i++)
    {
        args[i].arc = arc[i];
        args[i].tid = i;
        args[i].thr = (HANDLE)_beginthreadex(NULL, 0, worker_thread, &args[i], 0, NULL);
        if (args[i].thr <= 0)
        {
            exit(-1);
        }
    }

    /* tick as specified */
    remain_sec = run_sec;
    while (remain_sec > 0)
    {
        Sleep(1000);
        global_tick++;
        remain_sec--;
    }

    /* wait for the worker thread to finish */
    ok_to_run = 0;
    for (i = 0; i < num_thr; i++)
    {
        WaitForSingleObject(args[i].thr, INFINITE);
        arc_destroy(arc[i]);
    }

    free(args);
    return 0;
}

void test_commands(char *zk_addr, char *cluster_name)
{
    int ret, be_errno, i, j;
    arc_t *arc;
    arc_conf_t conf;
    arc_request_t *rqst;
    arc_reply_t *reply;

    arc_init_conf(&conf);
    conf.log_file_prefix = "arcci";
    conf.log_level = (arc_log_level_t)ARC_LOG_LEVEL_INFO;
    conf.log_file_prefix = NULL;
    conf.zk_reconnect_millis = 2000;
    conf.conn_reconnect_millis = 5000;
    conf.init_timeout_millis = 5000;
    conf.num_conn_per_gw = 256;
    conf.max_fd = 1024;

    arc = arc_new_zk(zk_addr, cluster_name, &conf);
    if (arc == NULL) {
        printf("arc_new_zk fail. errno:%d, winerror:%d\n", errno, WSAGetLastError());
        assert(arc != NULL);
    }

    printf("START\n");

    for (i = 0; i < 100; i++)
    {
        rqst = arc_create_request();
        assert(rqst != NULL);

        for (j = 0; j < 1; j++)
        {
            ret = arc_append_command(rqst, "set haha%d-%d hoho%d-%0d", i, j, i, j);
            assert(ret == 0);
        }

        ret = arc_do_request(arc, rqst, 3000, &be_errno);
        if (ret == 0)
        {
            while ((ret = arc_get_reply(rqst, &reply, &be_errno)) == 0
                && reply != NULL)
            {
                if (reply == NULL)
                {
                    OutputDebugString("reply == NULL");
                    break;
                }
                arc_free_reply(reply);
                reply = NULL;
            }
            if (ret != 0)
                assert(ret == 0);
        }
        else
        {
            reply = NULL;
            ret = arc_get_reply(rqst, &reply, &be_errno);
            if (ret != -1)
                assert(ret == -1);
            if (reply != NULL)
                assert(reply == NULL);
        }
        arc_free_request(rqst);
    }
    arc_destroy(arc);
}

void test_tripleS(char *zk_addr, char *cluster_name)
{
    int ret, be_errno, i, j, k, a;
    arc_t *arc;
    arc_conf_t conf;
    arc_request_t *rqst;
    arc_reply_t *reply;

    arc_init_conf(&conf);
    conf.log_file_prefix = "arcci";
    conf.log_level = (arc_log_level_t)ARC_LOG_LEVEL_INFO;
    conf.log_file_prefix = NULL;
    conf.zk_reconnect_millis = 2000;
    conf.conn_reconnect_millis = 5000;
    conf.init_timeout_millis = 5000;
    conf.num_conn_per_gw = 2;
    conf.max_fd = 0x7FFFF;

    arc = arc_new_zk(zk_addr, cluster_name, &conf);
    if (arc == NULL) {
        printf("arc_new_zk fail. errno:%d, winerror:%d\n", errno, WSAGetLastError());
        assert(arc != NULL);
    }

    for (i = 0; i < 1; i++) {
        for (j = 0; j < 50; j++) {
            rqst = arc_create_request();
            for (k = 0; k < 10; k++) {
                assert(rqst != NULL);

                for (a = 0; a < 2; a++)
                    ret = arc_append_command(rqst, "s3ladd * 20141017_key_%d field_%d name_%d value_%020d 86400000", i, j, a, k);
                assert(ret == 0);
            }

            ret = arc_do_request(arc, rqst, 3000, &be_errno);
            if (ret == 0)
            {
                while ((ret = arc_get_reply(rqst, &reply, &be_errno)) == 0
                    && reply != NULL)
                {
                    if (reply == NULL)
                    {
                        break;
                    }
                    arc_free_reply(reply);
                    reply = NULL;
                }
                assert(ret == 0);
            }
            else
            {
                reply = NULL;
                ret = arc_get_reply(rqst, &reply, &be_errno);
                if (ret != -1)
                {
                    volatile int a = 0;
                }
                if (reply != NULL)
                {
                    volatile int a = 0;
                }
            }

            arc_free_request(rqst);
        }
    }

    arc_destroy(arc);
}

void test_list(char *zk_addr, char *cluster_name)
{
    int ret, be_errno, i, j;
    arc_t *arc;
    arc_conf_t conf;
    arc_request_t *rqst;
    arc_reply_t *reply;

    arc_init_conf(&conf);
    conf.log_file_prefix = "arcci";
    conf.log_level = (arc_log_level_t)ARC_LOG_LEVEL_INFO;
    conf.log_file_prefix = NULL;
    conf.zk_reconnect_millis = 2000;
    conf.conn_reconnect_millis = 5000;
    conf.init_timeout_millis = 5000;
    conf.num_conn_per_gw = 2;
    conf.max_fd = 0x7FFFF;

    arc = arc_new_zk(zk_addr, cluster_name, &conf);
    if (arc == NULL) {
        printf("arc_new_zk fail. errno:%d, winerror:%d\n", errno, WSAGetLastError());
        assert(arc != NULL);
    }

    for (i = 0; i < 100; i++) {
        if (i % 100 == 0)
            printf("%d\n", i);

        rqst = arc_create_request();
        assert(rqst != NULL);
        for (j = 0; j < 120; j++) {
            ret = arc_append_command(rqst, "rpush key_%020d %0128d", i, j);
            assert(ret == 0);
        }

        ret = arc_do_request(arc, rqst, 3000, &be_errno);
        if (ret == 0)
        {
            while ((ret = arc_get_reply(rqst, &reply, &be_errno)) == 0 && reply != NULL)
            {
                if (reply == NULL)
                {
                    break;
                }
                arc_free_reply(reply);
                reply = NULL;
            }
            assert(ret == 0);
        }
        else
        {
            reply = NULL;
            ret = arc_get_reply(rqst, &reply, &be_errno);
            if (ret != -1)
            {
                volatile int a = 0;
            }
            if (reply != NULL)
            {
                volatile int a = 0;
            }
        }

        arc_free_request(rqst);
    }

    arc_destroy(arc);
}

void test_sortedset(char *zk_addr, char *cluster_name)
{
    int ret, be_errno, i, j;
    arc_t *arc;
    arc_conf_t conf;
    arc_request_t *rqst;
    arc_reply_t *reply;

    arc_init_conf(&conf);
    conf.log_file_prefix = "arcci";
    conf.log_level = (arc_log_level_t)ARC_LOG_LEVEL_INFO;
    conf.log_file_prefix = NULL;
    conf.zk_reconnect_millis = 2000;
    conf.conn_reconnect_millis = 5000;
    conf.init_timeout_millis = 5000;
    conf.num_conn_per_gw = 2;
    conf.max_fd = 0x7FFFF;

    arc = arc_new_zk(zk_addr, cluster_name, &conf);
    if (arc == NULL) {
        printf("arc_new_zk fail. errno:%d, winerror:%d\n", errno, WSAGetLastError());
        assert(arc != NULL);
    }

    for (i = 0; i < 100; i++) {
        if (i % 100 == 0)
            printf("%d\n", i);

        rqst = arc_create_request();
        assert(rqst != NULL);
        for (j = i; j < i + 10; j++) {
            const int d = i * 10 + j;
            ret = arc_append_command(rqst, "zadd key4_%099d %d %d", 0, d, d);
            assert(ret == 0);
        }

        ret = arc_do_request(arc, rqst, 3000, &be_errno);
        if (ret == 0)
        {
            while ((ret = arc_get_reply(rqst, &reply, &be_errno)) == 0 && reply != NULL)
            {
                if (reply == NULL)
                {
                    break;
                }
                arc_free_reply(reply);
                reply = NULL;
            }
            assert(ret == 0);
        }
        else
        {
            reply = NULL;
            ret = arc_get_reply(rqst, &reply, &be_errno);
            if (ret != -1)
            {
                volatile int a = 0;
            }
            if (reply != NULL)
            {
                volatile int a = 0;
            }
        }

        arc_free_request(rqst);
    }

    arc_destroy(arc);
}

int createDummyHandles(const int count)
{
    HANDLE hMutex = CreateMutex(NULL, FALSE, NULL);
    HANDLE hMutexDup = NULL;

    for (int i = 0; i < count; i++)
    {
        if (0 == DuplicateHandle(GetCurrentProcess(), hMutex, 
            GetCurrentProcess(), &hMutexDup, 0, FALSE, DUPLICATE_SAME_ACCESS)) {
            return -1;
        }
    }

    return 0;
}

const char *usage = "usage : arcci_win_example.exe <num_cluster> <zookeeper_address_1> <cluster_name_1> <num_thread_1> <zookeeper_address_2> <cluster_name_2> <num_thread_2> ... <run_time>\n";

int main(int argc, char **argv)
{
    int ret = 0;
    int num_cluster = 0;
    char **zk_addr;
    char **cluster_name;
    int *num_thr;
    int num_thr_tot;
    int i, j;
    int argv_idx;
    arc_t *arc;
    int widx;
    struct worker_arg *workers = NULL;

    if (argc < 4)
    {
    	printf(usage);
    	exit(-1);
    }

    num_cluster = atoi(argv[1]);
    if (num_cluster <= 0 || argc != num_cluster * 3 + 2)
    {
        printf(usage);
        exit(-1);
    }

    widx = 0;
    num_thr_tot = 0;
    num_thr = (int*)malloc(sizeof(int) * num_cluster);
    zk_addr = (char**)malloc(sizeof(char*) * num_cluster);
    cluster_name = (char**)malloc(sizeof(char*) * num_cluster);
    for (i = 0; i < num_cluster; i++)
    {
        argv_idx = 3 * i + 1;
        zk_addr[i] = argv[argv_idx + 1];
        cluster_name[i] = argv[argv_idx + 2];
        num_thr[i] = atoi(argv[argv_idx + 3]);
        num_thr_tot += num_thr[i];
    }

    ok_to_run = 1;
    InitializeCriticalSection(&global_lock);
    workers = (struct worker_arg*)malloc(sizeof(struct worker_arg) * num_thr_tot);
    assert(workers != NULL);

    buf_large = (char*)malloc(BUF_LARGE_SIZE);
    memset(buf_large, 'A', BUF_LARGE_SIZE);
    buf_large[BUF_LARGE_SIZE - 1] = '\0';

    createDummyHandles(50000);

    for (i = 0; i < num_cluster; i++)
    {
        arc_conf_t conf;
        arc_init_conf(&conf);
        conf.log_level = ARC_LOG_LEVEL_INFO;
        conf.zk_reconnect_millis = 3000;
        conf.init_timeout_millis = 5000;
        conf.conn_reconnect_millis = 5000;
        conf.num_conn_per_gw = 2;
        conf.max_fd = 1024;

        /* launch workers */
        for (j = 0; j < num_thr[i]; j++)
        {
            /*
            char log_prefix[64];
            _snprintf(log_prefix, 64, "arcci_%s_%d", cluster_name[i], j);
            conf.log_file_prefix = log_prefix;
            */
            conf.log_file_prefix = NULL;

            arc = arc_new_zk(zk_addr[i], cluster_name[i], &conf);
            assert(arc != NULL);

            workers[widx].arc = arc;
            workers[widx].tid = i;
            workers[widx].thr = (HANDLE)_beginthreadex(NULL, 0, worker_thread, &workers[widx], 0, NULL);
            if (workers[widx].thr <= 0)
            {
                exit(-1);
            }
            widx++;
        }
    }

    for (int i = 0; i < 10; i++) {
        printf("(%d/10)", i);
        getchar();
    }

    /* wait for the worker thread to finish */
    ok_to_run = 0;
    for (i = 0; i < num_thr_tot; i++)
    {
        WaitForSingleObject(workers[i].thr, INFINITE);
        arc_destroy(workers[i].arc);
    }

    free(zk_addr);
    free(cluster_name);
    free(num_thr);
    free(workers);
    free(buf_large);
    DeleteCriticalSection(&global_lock);
    return ret;
}
