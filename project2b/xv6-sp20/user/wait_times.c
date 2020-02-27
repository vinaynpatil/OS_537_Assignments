#include "types.h"
#include "stat.h"
#include "user.h"
#include "pstat.h"
#define check(exp, msg) if(exp) {} else {				\
  printf(1, "%s:%d check (" #exp ") failed: %s\n", __FILE__, __LINE__, msg); \
  exit();}

int wait_times[] = {640, 320, 160, 80};

int workload(int n) {
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

  int i, j, k;
  for (i = 0; i <= 60; i++) {
    if (fork() == 0) {
      workload(400000 * (i + 1));
      if (i == NPROC - 4) {
        sleep(100);
        check(getprocinfo(&st) == 0, "getprocinfo");
        for(k = 0; k < NPROC; k++) {
          if (st.inuse[k]) {
            int m;
            printf(1, "pid: %d\n", st.pid[k]);
            if (st.pid[k] > 3) {
              for(j = 0; j<3; j++)
                check(st.wait_ticks[k][j] <= wait_times[j], "The wait times should not exceed the expected times at a given priority i.e. 10x");
            }
            for (m = 3; m >= 0; m--) {
              printf(1, "\t level %d wait ticks used %d\n", m, st.wait_ticks[k][m]);
            }
          }
        }
        
        printf(1, "TEST PASSED\n");  
      }
    } else {
      wait();
      break;
    }
  }

  exit();
}
