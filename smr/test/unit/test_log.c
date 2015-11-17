#include <stdlib.h>
#include <limits.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>

#include "smr.h"
#include "smr_log.h"

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */

// ---- setup, tear down
static int setup_dir (void);
static void remove_dir (void);

// ---- log write utilities
// returns size written
static int
log_data (smrLog * handle, smrLogAddr * addr, int sid, int hash, char *data,
	  int size);

// ---- test cases

// log scan test
struct s_100th
{
  int count;
  long long seq;
};
#define init_s_100th(s)  do { \
    (s)->count = 0;           \
    (s)->seq = 0LL;           \
} while(0)

static int scanner_100th (void *arg, long long seq, long long timestamp,
			  int hash, unsigned char *buf, int size);
static void test_logscan (void);

/* --------------- */
/* LOCAL VARIABLES */
/* --------------- */
static char tempDirBuf[8192];
static char *tempDir = NULL;


/* ------------------------- */
/* LOCAL FUNCTION DEFINITION */
/* ------------------------- */
static int
setup_dir (void)
{
  if (tempDir != NULL)
    {
      return -1;
    }
  sprintf (tempDirBuf, "dirXXXXXX");

  tempDir = mkdtemp (tempDirBuf);
  if (tempDir == NULL)
    {
      return -1;
    }

  return 0;
}


static void
remove_dir (void)
{
  char command[8192];

  if (tempDir == NULL)
    {
      return;
    }
  sprintf (command, "rm -rf %s", tempDir);
  system (command);
  tempDir = NULL;
}

//
// Note: this function does not handle fragmented write over several log files
// Use this function only for test purpose.
//
static int
log_data (smrLog * handle, smrLogAddr * addr, int sid, int hash, char *data,
	  int size)
{
  int ret;
  int length;
  char buf[1 + 3 * sizeof (int) + sizeof (long long)];

  if (data == NULL || size <= 0)
    {
      return -1;
    }

  buf[0] = SMR_OP_SESSION_DATA;
  sid = htonl (sid);
  memcpy (&buf[1], &sid, sizeof (int));
  hash = htonl (hash);
  memcpy (&buf[1 + sizeof (int)], &hash, sizeof (int));
  memset (&buf[1 + 2 * sizeof (int)], 0, sizeof (long long));
  length = htonl (size);
  memcpy (&buf[1 + 2 * sizeof (int) + sizeof (long long)], &length,
	  sizeof (int));

  ret = smrlog_append (handle, addr, buf, sizeof (buf));
  assert (ret == sizeof (buf));

  ret = smrlog_append (handle, addr, data, size);
  assert (ret == size);

  return sizeof (buf) + size;
}

static int
scanner_100th (void *arg, long long seq, long long timestamp, int hash,
	       unsigned char *buf, int size)
{
  struct s_100th *s = (struct s_100th *) arg;

  assert (s != NULL);
  assert (s->count < 100);

  s->count++;
  if (s->count == 100)
    {
      s->seq = seq;
      return 0;
    }

  return 1;
}


static void
test_logscan (void)
{
  smrLog *smrlog;
  smrLogAddr *addr;
  int i, ret;

  assert (tempDir != NULL);
  smrlog = smrlog_init (tempDir);
  assert (smrlog != NULL);

  // Make a log file that contains 1000 entries.
  // Find sequence number of 100th entry.
  addr = smrlog_write_mmap (smrlog, 0LL, 1);
  assert (addr != NULL);

  for (i = 0; i < 1000; i++)
    {
      ret = log_data (smrlog, addr, 0, 1234, "abc", 3);
      assert (ret > 0);
    }

  ret = smrlog_sync (smrlog, addr);
  assert (ret == 0);
  smrlog_munmap (smrlog, addr);

  // TEST
  do
    {
      struct s_100th st;

      init_s_100th (&st);
      ret = smrlog_scan (smrlog, 0LL, -1LL, scanner_100th, &st);
      assert (ret == 0);
      assert (st.count == 100);
    }
  while (0);


  // remove log file
  ret = smrlog_purge_after (smrlog, 0LL);
  assert (ret == 0);

  smrlog_destroy (smrlog);
}

static void
test_purge (void)
{
  smrLog *smrlog;
  smrLogAddr *addr;
  int i, ret, count;
  long long seq = 0LL;
  //int write_sizes[] = {8, 31, 127, 222, 409, 512, 1022, 2001, 8099};
  //int write_sizes[] = { 127, 2001, 8099 };
  int write_sizes[] = { 8099 };

  assert (tempDir != NULL);

  smrlog = smrlog_init (tempDir);
  assert (smrlog != NULL);

  for (i = 0; i < sizeof (write_sizes) / sizeof (int); i++)
    {
      int offset, remain, avail;
      int wsz = write_sizes[i];
      char *buf = NULL;

      addr = smrlog_write_mmap (smrlog, 0LL, 1);
      assert (addr != NULL);

      offset = smrlog_get_offset (smrlog, addr);
      assert (offset == 0);

      // append log wsz, rewind randomly
      count = 0;
      while (seq < SMR_LOG_FILE_DATA_SIZE - wsz)
	{
	  if (count % 10 == 0)
	    {
	      // fake purge (p=0.1)
	      ret = smrlog_purge_after (smrlog, seq);
	      assert (ret == 0);

	    }
	  else if (count % 10 < 5 && seq > 0)
	    {
	      // purge (p=0.4)
	      int r = rand () % wsz;
	      ret = smrlog_purge_after (smrlog, seq - r);
	      assert (ret == 0);
	      seq = seq - r;
	    }
	  else
	    {
	      int j;
	      // append (p=0.5)
	      ret = smrlog_get_buf (smrlog, addr, &buf, &avail);
	      assert (ret == 0);
	      assert (avail > wsz);
	      for (j = 0; j < wsz; j++)
		{
		  buf[j] = 'a';
		}
	      ret = smrlog_append (smrlog, addr, buf, wsz);
	      assert (ret == wsz);
	      seq = seq + wsz;
	    }

	  offset = smrlog_get_offset (smrlog, addr);
	  assert (offset == seq);

	  ret = smrlog_get_buf (smrlog, addr, &buf, &avail);
	  assert (ret == 0);
	  assert (buf != NULL);
	  assert ((char *) buf - (char *) addr->addr == (int) seq);
	  assert (avail == SMR_LOG_FILE_DATA_SIZE - seq);

	  remain = smrlog_get_remain (smrlog, addr);
	  assert (remain == SMR_LOG_FILE_DATA_SIZE - seq);

	  count++;
	}
      // remove log file
      ret = smrlog_purge_after (smrlog, 0LL);
      assert (ret == 0);
    }

  smrlog_destroy (smrlog);
}

/* ---- */
/* MAIN */
/* ---- */
int
main (int argc, char *argv[])
{
  int ret = 0;
  pid_t child;

  // setup directory
  ret = setup_dir ();
  if (ret < 0)
    {
      return ret;
    }

  child = fork ();

  if (child == -1)
    {
      return -1;		// failed to fork
    }
  else if (child > 0)		// parent
    {
      int status = 0;

      waitpid (child, &status, 0);

      // tear down
      remove_dir ();

      return ret;
    }
  else				// child
    {
      assert (child == 0);
      test_logscan ();
      test_purge ();
    }

  return 0;
}
