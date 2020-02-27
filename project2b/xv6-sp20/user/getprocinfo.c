#include "types.h"
#include "stat.h"
#include "user.h"
#include "pstat.h"

#define check(exp, msg) if(exp) {} else {\
  printf(1, "%s:%d check (" #exp ") failed: %s\n", __FILE__, __LINE__, msg);\
  exit();}

int
main(int argc, char *argv[])
{
  struct pstat st;
  check(getprocinfo(&st) == 0, "getporcinfo");

  int i, j;
  int pid = getpid();
  int found_self = 0;
  for(i = 0; i < NPROC; i++) {
    if (st.inuse[i]) {
      printf(1, "pid: %d priority: %d\n ", st.pid[i], st.priority[i]);
      if (st.pid[i] == pid) {
        found_self = 1;
      }
      
      for (j = 3; j >= 0; j--) {
        printf(1, "\t level %d ticks used %d\n", j, st.ticks[i][j]); 
        printf(1, "\t level %d ticks waited for %d\n", j, st.wait_ticks[i][j]); 
      }
    }
  }


  check(found_self, "cannot find tester process running.")
  printf(1, "TEST PASSED");

  exit();
}
