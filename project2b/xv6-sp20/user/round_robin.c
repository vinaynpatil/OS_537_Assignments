#include "types.h"
#include "stat.h"
#include "user.h"
#include "pstat.h"

#define check(exp, msg) if(exp) {} else {\
  printf(1, "%s:%d check (" #exp ") failed: %s\n", __FILE__, __LINE__, msg);\
  exit();}

#define DDEBUG 1

#ifdef DDEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif


int workload(int n) {
  int i, j = 0;
  for (i = 0; i < n; i++) {
    j += i * j + 1;
  }
  return j;
}

int
main(int argc, char *argv[])
{
  struct pstat st;
  int time_slices[] = {0, 32, 16, 8};
  check(getprocinfo(&st) == 0, "getprocinfo");

  // Push this thread to the bottom
  workload(80000000);

  int i, j, k;

  // Launch the 4 processes
  for (i = 0; i < 4; i++) {
    int c_pid = fork();
    // Child
    if (c_pid == 0) {
      workload(80000000);
      exit();
    }   
  }

  // Checking every 4 time-slice for 4 times
  for (i = 0; i < 4; i++) { 
    sleep(100);
    check(getprocinfo(&st) == 0, "getprocinfo");

    for (j = 0; j < NPROC; j++) {
      if (st.inuse[j] && st.pid[j] > 3) {
  
        DEBUG_PRINT((1, "pid: %d\n", st.pid[j]));
        for (k = 3; k >= 0; k--) {
          DEBUG_PRINT((1, "\t level %d ticks used %d\n", k, st.ticks[j][k]));
	        DEBUG_PRINT((1, "\t level %d waited for ticks %d\n", k, st.wait_ticks[j][k]));
          if (k > 0) {
            check(st.ticks[j][k] % time_slices[k] == 0, "All processes have used up the timer ticks in level 3, 2 and 1");
          }
        }
      } 
    }
  }

  for (i = 0; i < 4; i++) {
    wait();
  }

  printf(1, "TEST PASSED");

  exit();
}
