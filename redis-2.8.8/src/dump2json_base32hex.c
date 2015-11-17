#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <assert.h>
#include "dump_plugin.h"

/*  base64/normal alignment:
 *     0      1       2      3    
 * |123456|12 3456|1234 56|123456|
 * |123456 78|1234 5678|12 345678|
 *      0         1         2      
 */

/* base32/normal alignment:
 *
 *    0     1      2      3     4      5      6     7
 * |12345|123 45|12345|1 2345|1234 5|12345|12 345|12345|
 * |12345 678|12 34567 8|1234 5678|1 23456 78|123 45678|
 *      0          1          2         3          4
 *
 * normal byte 0 is (base32[0] << 3) | (base32[1] >> 2)
 *    masks: F8; 07
 * normal byte 1 is ((base32[1]&0x03) << 6) | (base32[2] << 1) | (base32[3] >> 4)
 *    masks: C0; 3E; 01
 * normal byte 2 is ((base32[3]&0x0F) << 4) | (base32[4] >> 1)
 *    masks: F0; 0F
 * normal byte 3 is ((base32[4]&0x01) << 7) | (base32[5] << 2) | (base32[6] >> 3)
 *    masks: 80; 7C; 03
 * normal byte 4 is ((base32[6]&0x07) << 5) | base32[7]
 *    masks: E0; 1F
 * 
 * base32 byte 0 is (normal[0] >> 3)
 * base32 byte 1 is ((normal[0]&0x07) << 2) | (normal[1] >> 6)
 * base32 byte 2 is ((normal[1] >> 1)&0x1f)
 * base32 byte 3 is ((normal[1]&0x01) << 4) | (normal[2] >> 4)
 * base32 byte 4 is ((normal[2]&0x0f) << 4) | (normal[3] >> 7) 
 * base32 byte 5 is ((normal[3] >> 2)&0x1f)
 * base32 byte 6 is ((normal[3]&0x03) << 3) | (normal[4] >> 5)
 * base32 byte 7 is ((normal[3]&0x1f) 
 */
static void fwrite_base32hex_nonquote(char *p, size_t len, FILE *fp)
{
    int out_len = 8*(len/5);
    int bufsize, i;
    int byte = 0;
    unsigned char s[8193];
    unsigned char *buf, *start;

    switch (len % 5) {
        case 1: out_len += 2; break;
        case 2: out_len += 4; break;
        case 3: out_len += 5; break;
        case 4: out_len += 7; break;
    }

    bufsize = 8*(len/5) + (len % 5 == 0 ? 0 : 8);

    if (bufsize < 8192) {
        buf = s;
    } else {
        buf = malloc(bufsize+1);
    }

    start = buf;

    while (len--) {
        unsigned char n = *p;
        switch (byte) {
            case 0:
                buf[0] = n >> 3;
                buf[1] = (n & 0x07) << 2;
                break;
            case 1:
                buf[1] |= (n >> 6) & 0x03;
                buf[2] = (n >> 1) & 0x1f;
                buf[3] = (n & 0x01) << 4;
                break;
            case 2:
                buf[3] |= (n >> 4) & 0x0f;
                buf[4] = (n & 0x0f) << 1;
                break;
            case 3:
                buf[4] |= (n >> 7) & 0x01;
                buf[5] = (n >> 2) & 0x1f;
                buf[6] = (n & 0x03) << 3;
                break;
            case 4:
                buf[6] |= (n >> 5) & 0x07;
                buf[7] = n & 0x1f;
                break;
        }
        p++;
        byte = (byte+1) % 5;
        if (byte == 0) {
            buf += 8;
        }
    }

    buf = start;

    for (i = 0; i < bufsize; i++) {
        if (i < out_len) {
            if (buf[i] < 10) {
                buf[i] += '0';
            } else if (buf[i] < 32) {
                buf[i] += 'a' - 10;
            } else {
                assert(0);
            }
        } else {
            buf[i] = '=';
        }
    }
    buf[bufsize] = '\0';

    fwrite(buf, bufsize, 1, fp);

    if (bufsize >= 8192) {
        free(buf);
    }
}

static void fwrite_base32hex(char *p, size_t len, FILE *fp)
{
    fputc('\"', fp);
    fwrite_base32hex_nonquote(p, len, fp);
    fputc('\"', fp);
}

int initialize(int argc, char **argv, void **pctx)
{
    FILE *fp;

    if (argc != 1) return RET_ERR;

    fp = fopen(argv[0], "w");
    if (fp == NULL) return RET_ERR;

    *pctx = fp;
    return RET_OK;
}

int dumpinfo (void *ctx, int rdbver, long long smr_seqnum, long long smr_mstime)
{
    printf("Dump Start, rdbver:%d, smr_seqnum:%lld, smr_mstime:%lld\r\n", rdbver, smr_seqnum, smr_mstime);
    return RET_OK;
}

int begin_key (void *ctx, int type, char *key, int keylen, long long expiretime)
{
    FILE *fp = ctx;

    fputs("{\"key\":", fp);
    fwrite_base32hex(key, keylen, fp);
    fprintf(fp, " ,\"expire\":%lld,", expiretime);
 
    switch (type) {
        case PLUGIN_RDB_TYPE_STRING:
            fputs("\"type\":\"string\",\"value\":", fp);
            break;
        case PLUGIN_RDB_TYPE_LIST:
            fputs("\"type\":\"list\",\"value\":[", fp);
            break;
        case PLUGIN_RDB_TYPE_SET:
            fputs("\"type\":\"set\",\"value\":[", fp);
            break;
        case PLUGIN_RDB_TYPE_ZSET:
            fputs("\"type\":\"zset\",\"value\":[", fp);
            break;
        case PLUGIN_RDB_TYPE_HASH:
            fputs("\"type\":\"hash\",\"value\":[", fp);
            break;
        case PLUGIN_RDB_TYPE_SSS:
            fputs("\"type\":\"sss\",\"value\":[", fp);
            break;
    }
    return RET_OK;
}

int string_val (void *ctx, char *val, int vallen)
{
    FILE *fp = ctx;

    fwrite_base32hex(val, vallen, fp);
    return RET_OK;
}

int list_val (void *ctx, char *val, int vallen)
{
    FILE *fp = ctx;
    
    fwrite_base32hex(val, vallen, fp);
    fputc(',', fp);
    return RET_OK;
}

int set_val (void *ctx, char *val, int vallen)
{
    FILE *fp = ctx;
    
    fwrite_base32hex(val, vallen, fp);
    fputc(',', fp);
    return RET_OK;
}

int zset_val (void *ctx, char *val, int vallen, double score)
{
    FILE *fp = ctx;
    
    fputs("{\"data\":", fp);
    fwrite_base32hex(val, vallen, fp);
    fprintf(fp, ",\"score\":%.17g},", score);
    return RET_OK;
}

int hash_val (void *ctx, char *hkey, int hkeylen, char *hval, int hvallen)
{
    FILE *fp = ctx;
    
    fputs("{\"hkey\":", fp);
    fwrite_base32hex(hkey, hkeylen, fp);
    fputs(",\"hval\":", fp);
    fwrite_base32hex(hval, hvallen, fp);
    fputs("},",fp);
    return RET_OK;
}

int begin_sss_collection (void *ctx, char *ks, int kslen, char *svc, int svclen, char *key, int keylen, int mode)
{
    FILE *fp = ctx;

    fputs("{\"ks\":", fp);
    fwrite_base32hex(ks, kslen, fp);
    fputs(",\"field\":", fp);
    fwrite_base32hex(svc, svclen, fp);
    fputs(",\"name\":", fp);
    fwrite_base32hex(key, keylen, fp);
    if (mode == PLUGIN_SSS_KV_LIST) {
        fputs(",\"mode\":\"list\",\"value\":[", fp);
    } else {
        fputs(",\"mode\":\"set\",\"value\":[", fp);
    }

    return RET_OK;
}

int sss_val (void *ctx, char *val, int vallen, long long val_expire)
{
    FILE *fp = ctx;

    fputs("{\"data\":", fp);
    fwrite_base32hex(val, vallen, fp);
    fprintf(fp, ",\"expire\":%lld},", val_expire);
    return RET_OK;
}

int end_sss_collection (void *ctx, int mode)
{
    FILE *fp = ctx;

    fseek(fp, -1, SEEK_CUR);
    fputs("]},", fp);

    return RET_OK;
}

int end_key (void *ctx, int type)
{
    FILE *fp = ctx;

    fseek(fp, -1, SEEK_CUR);
    switch (type) {
        case PLUGIN_RDB_TYPE_STRING:
            fputs("\"}\n", fp);
            break;
        case PLUGIN_RDB_TYPE_LIST:
        case PLUGIN_RDB_TYPE_SET:
        case PLUGIN_RDB_TYPE_ZSET:
        case PLUGIN_RDB_TYPE_HASH:
        case PLUGIN_RDB_TYPE_SSS:
            fputs("]}\n", fp);
            break;
    }
    return RET_OK;
}

int finalize (void *ctx)
{
    FILE *fp = ctx;

    fclose(fp);
    return RET_OK;
}

struct dump_plugin_callback callback = {
    initialize,
    dumpinfo,
    begin_key,
    string_val,
    list_val,
    set_val,
    zset_val,
    hash_val,
    begin_sss_collection,
    sss_val,
    end_sss_collection,
    end_key,
    finalize
};

