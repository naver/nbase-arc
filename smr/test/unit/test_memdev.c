/*
 * Copyright 2015 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
#include "log_internal.h"

/* -------------------------- */
/* LOCAL FUNCTION DECLARATION */
/* -------------------------- */

// ---- setup, tear down
static int setup_dir (void);
static void remove_dir (void);

// ---- memdev test
static void test_memdev (void);

/* --------------- */
/* LOCAL VARIABLES */
/* --------------- */
static char tempDirBuf[8192];
static char *tempDir = NULL;

#define MAX_LINES 8192
static char lineCov[MAX_LINES];
static FILE *covFp = NULL;
static int testState = 0;


/* ------------------------- */
/* LOCAL FUNCTION DEFINITION */
/* ------------------------- */
static int
setup_dir (void)
{
  int ret;
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

  ret = create_mem_dev (tempDir);
  assert (ret == 0);

  return 0;
}

static void
remove_dir (void)
{
  int ret;
  char command[8192];

  if (tempDir == NULL)
    {
      return;
    }
  ret = unlink_mem_dev (tempDir);
  assert (ret == 0);

  sprintf (command, "rm -rf %s", tempDir);
  system (command);
  tempDir = NULL;
}

#ifdef SFI_ENABLED
static void
coverLine (int line)
{
  int i;
  fprintf (covFp, "l %d\n", line);
  fflush (covFp);
  lineCov[line] = 1;

  printf ("coverage ");
  for (i = 0; i < MAX_LINES; i++)
    {
      if (lineCov[i])
	{
	  printf (" %d", i);
	}
    }
  printf ("\n");
}
#endif

static void
nextState (int state)
{
  char *buf = NULL;

  printf ("nextState:%d\n", state);
  fprintf (covFp, "s %d\n", state);
  fflush (covFp);
  printf
    ("(State:%d next:%d)=======================================================\n",
     testState, state);
  (void) info_mem_dev (tempDir, &buf);
  if (buf != NULL)
    {
      printf ("%s\n", buf);
      free (buf);
    }
  testState = state;
}

#ifdef SFI_ENABLED
static void
crasher (char *file, int line)
{
  if (lineCov[line])
    {
      return;
    }
  coverLine (line);

  //Do segfault (TODO)
  *(char *) (0) = 1;
}
#endif

static void
test_memdev (void)
{
  int i;
  char cov_file[8192];
  char *line = NULL;
  size_t len = 0;
  ssize_t nr = 0;
  logDev *dev = NULL;

  for (i = 0; i < MAX_LINES; i++)
    {
      lineCov[i] = 0;
    }

  //
  // open coverage file and read coverage
  // 
  sprintf (cov_file, "%s/memdev.cov", tempDir);
  covFp = fopen (cov_file, "a+");
  assert (covFp != NULL);

  while ((nr = getline (&line, &len, covFp)) != -1)
    {
      int n;

      if (line[0] == 'l')
	{
	  n = atoi (&line[2]);
	  assert (n > 0);
	  assert (n < MAX_LINES);
	  lineCov[n] = 1;
	}
      else if (line[0] == 's')
	{
	  testState = atoi (&line[2]);
	  assert (testState >= 0);
	}
      else
	{
	  assert (0);
	}
    }
  if (line)
    {
      free (line);
    }

#ifdef SFI_ENABLED
  //
  // setup callback
  //
  sfi_mshmcs_register (crasher);
#endif


  // 
  // open memory device
  //
  dev = open_mem_dev (tempDir);
  assert (dev != NULL);

  while (1)
    {
      smrLogAddr *addr = NULL;
      int ret;

      switch (testState)
	{
	case 0:
	  // create log file (0) (0,-1,-1)(0,-1,-1)
	  addr = dev->get_mmap (dev, 0 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	  if (addr == NULL)
	    {
	      printf ("errno:%d\n", errno);
	      assert (0);
	    }
	  dev->munmap (dev, addr);
	  nextState (1);
	  break;
	case 1:
	  // create log file(1) (0,1,-1)(0,1,-1)
	  addr = dev->get_mmap (dev, 1 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	  assert (addr != NULL);
	  dev->munmap (dev, addr);
	  nextState (2);
	  break;
	case 2:
	  // create log file(2) (0,1,2)(0,1,2)
	  addr = dev->get_mmap (dev, 2 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	  assert (addr != NULL);
	  dev->munmap (dev, addr);
	  nextState (3);
	  break;
	case 3:
	  // create log file (3) --> ENOSPC
	  addr = dev->get_mmap (dev, 3 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	  assert (addr == NULL);
	  assert (errno == ERRNO_NO_SPACE);
	  nextState (4);
	  break;
	case 4:
	  {
	    long long *seqs = NULL;
	    int seqs_size = 0;

	    ret = dev->get_seqs (dev, &seqs, &seqs_size);
	    assert (ret == 0);
	    assert (seqs_size == 3);
	    free (seqs);
	  }
	  nextState (5);
	  break;
	case 5:
	  // sync (0)
	  ret = dev->synced (dev, 0 * SMR_LOG_FILE_DATA_SIZE);
	  assert (ret == 0);
	  nextState (6);
	  break;
	case 6:
	  // delete(0)
	  ret = dev->purge (dev, 0 * SMR_LOG_FILE_DATA_SIZE);
	  assert (ret == 0);
	  nextState (7);
	case 7:
	  // create log file (3) --> (1,2,3)(1,2,3)
	  addr = dev->get_mmap (dev, 3 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	  assert (addr != NULL);
	  dev->munmap (dev, addr);
	  nextState (8);
	  break;
	case 8:
	  {
	    // sync 1,2,3  (for the log to be sacrificed)
	    ret = dev->synced (dev, 1 * SMR_LOG_FILE_DATA_SIZE);
	    assert (ret == 0);
	    ret = dev->synced (dev, 2 * SMR_LOG_FILE_DATA_SIZE);
	    assert (ret == 0);
	    ret = dev->synced (dev, 3 * SMR_LOG_FILE_DATA_SIZE);
	    assert (ret == 0);
	  }
	  nextState (9);
	  break;
	case 9:
	  {
	    smrLogAddr *o1 = NULL, *o2 = NULL, *o3 = NULL;
	    smrLogAddr *n1 = NULL, *n2 = NULL, *n3 = NULL;

	    // open 1,2,3 (handle can be null due to segfault at creation 4,5,6)
	    o1 = dev->get_mmap (dev, 1 * SMR_LOG_FILE_DATA_SIZE, 1, 0);
	    o2 = dev->get_mmap (dev, 2 * SMR_LOG_FILE_DATA_SIZE, 1, 0);
	    o3 = dev->get_mmap (dev, 3 * SMR_LOG_FILE_DATA_SIZE, 1, 0);
	    // create 4,5,6
	    n1 = dev->get_mmap (dev, 4 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	    assert (n1 != NULL);
	    n2 = dev->get_mmap (dev, 5 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	    assert (n2 != NULL);
	    n3 = dev->get_mmap (dev, 6 * SMR_LOG_FILE_DATA_SIZE, 0, 1);
	    assert (n3 != NULL);
	    // close 1,2,3,4,5,6
	    if (o1)
	      {
		ret = dev->munmap (dev, o1);
		assert (ret == 0);
	      }
	    if (o2)
	      {
		ret = dev->munmap (dev, o2);
		assert (ret == 0);
	      }
	    if (o3)
	      {
		ret = dev->munmap (dev, o3);
		assert (ret == 0);
	      }
	    if (n1)
	      {
		ret = dev->munmap (dev, n1);
		assert (ret == 0);
	      }
	    if (n2)
	      {
		ret = dev->munmap (dev, n2);
		assert (ret == 0);
	      }
	    if (n3)
	      {
		ret = dev->munmap (dev, n3);
		assert (ret == 0);
	      }
	  }
	  nextState (10);
	  break;
	case 10:
	  //delete 4,5,6
	  ret = dev->purge (dev, 4 * SMR_LOG_FILE_DATA_SIZE);
	  assert (ret == 0);
	  ret = dev->purge (dev, 5 * SMR_LOG_FILE_DATA_SIZE);
	  assert (ret == 0);
	  ret = dev->purge (dev, 6 * SMR_LOG_FILE_DATA_SIZE);
	  assert (ret == 0);
	  nextState (99);
	  break;
	case 99:
	  printf ("Test done!\n");
	  goto outer;
	default:
	  printf ("Error invalid state:%d\n", testState);
	  goto outer;
	}
    }
outer:
  //
  // close memory device
  //
  dev->close (dev);
  fclose (covFp);

  return;
}

/* ---- */
/* MAIN */
/* ---- */
#define MAX_CHILD_FORK 100
int
main (int argc, char *argv[])
{
  int ret = 0;
  int count = 0;
  pid_t child;
  int status = 0;

  // setup directory
  ret = setup_dir ();
  if (ret < 0)
    {
      return ret;
    }

  while (count < MAX_CHILD_FORK)
    {
      printf ("Loop %d (%d)\n", count, status);

      child = fork ();
      if (child == -1)
	{
	  printf ("Failed to fork\n");
	  return -1;		// failed to fork
	}
      else if (child > 0)	// parent
	{

	  status = 0;
	  waitpid (child, &status, 0);
	  if (status == 0)
	    {
	      goto done;
	    }
	}
      else			// child
	{
	  assert (child == 0);
	  test_memdev ();
	  return 0;
	}
      count++;
    }

done:
  remove_dir ();

  if (count == MAX_CHILD_FORK)
    {
      printf ("Err\n");
      return -1;
    }
  else
    {
      printf ("Ok\n");
      return 0;
    }
}
