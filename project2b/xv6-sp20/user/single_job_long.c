#include "types.h"
#include "stat.h"
#include "user.h"
#include "pstat.h"
#define check(exp, msg) if(exp) {} else {\
  printf(1, "%s:%d check (" #exp ") failed: %s\n", __FILE__, __LINE__, msg);\
  exit();}

int long_workload(int n) {
  int i, j = 0;
  for(i = 0; i < n; i++)
    j += i * j + 1;
  return j;
}

int
main(int argc, char *argv[])
{
  struct pstat st;

  sleep(10);

  int i = long_workload(80000000), j;
  check(getprocinfo(&st) == 0, "getprocinfo");

  int pid = getpid(); 
  for(i = 0; i < NPROC; i++) {
    if (st.inuse[i]) {
      printf(1, "pid: %d\n", st.pid[i]);
      if (pid == st.pid[i]) {
        for (j = 3; j >= 0; j--) {
          printf(1, "\t level %d ticks used %d\n", j, st.ticks[i][j]); 
//	  printf(1, "\t level %d ticks waited for  %d\n", j, st.wait_ticks[i][j]); 
        }

        check(st.ticks[i][3] == 8,
            "current process should run for 8 ticks in level 3");
        check(st.wait_ticks[i][3] == 0,
	    "current process should not have waited in level 3");
        check(st.ticks[i][2] == 16,
            "current process should run for 16 ticks in level 2");
        check(st.wait_ticks[i][2] == 0,
            "current process should not have waited in level 2");
        check(st.ticks[i][1] == 32,
            "current process should run for 32 ticks in level 1");
	check(st.wait_ticks[i][1] == 0,
            "current process should not have waited in level 1");
        check(st.ticks[i][0] > 0,
            "current process should run for at least 1 tick in the lowest level");
        check(st.priority[i] == 0, "current process should have the lowest priority");
      } else {
        for (j = 3; j >= 0; j--) {
          printf(1, "\t level %d ticks used %d\n", j, st.ticks[i][j]);  
        }
      }
    }
  }

  printf(1, "TEST PASSED");
  exit();
}
