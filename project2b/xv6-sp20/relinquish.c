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

//char buf[10000]; // ~10KB
int workload(int n, int t) {
  int i, j = 0;
  for (i = 0; i < n; i++) {
    j += i * j + 1;
  }

  if (t > 0) sleep(t);
  for (i = 0; i < n; i++) {
    j += i * j + 1;
  }
  return j;
}

int
main(int argc, char *argv[])
{
  struct pstat st;
  int time_slices[] = {64, 32, 16, 8};
  check(getprocinfo(&st) == 0, "getprocinfo");

  // Push this thread to the bottom
  workload(80000000, 0);

  int i, j, k;

  for (i = 0; i < 2; i++) {
    int c_pid = fork();
    int t = 0;
    // Child
    if (c_pid == 0) {
      if (i == 1) {
          t = 64*10;
      }
      workload(160000000, t);
      exit();
    }
  }

  for (i = 0; i < 12; i++) { 
    sleep(10);
    check(getprocinfo(&st) == 0, "getprocinfo");
    
    for (j = 0; j < NPROC; j++) {
      if (st.inuse[j] && st.pid[j] > 3) {
  
        DEBUG_PRINT((1, "pid: %d\n", st.pid[j]));
        for (k = 3; k >= 0; k--) {
          DEBUG_PRINT((1, "\t level %d ticks used %d\n", k, st.ticks[j][k]));
          if (k > 0 && st.state[j] != 5) { 
            check(st.ticks[j][k] % time_slices[k] == 0, "Timer ticks above level 0 should be used up\n"); 
          }
        }
      } 
    }
  }

  for (i = 0; i < 2; i++) {
    wait();
  }

  printf(1, "TEST PASSED");

  exit();
}
