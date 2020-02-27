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

  sleep(10);

  check(getprocinfo(&st) == 0, "getprocinfo");

  int count = 0; 

  // a weak test, only show whether if the current process gets run once
  int i;
  for(i = 0; i < NPROC; i++) {
    if (st.inuse[i]) {
      count++;
    }
  }

  check(count == 3, "should be three processes: init, sh, tester ...");

  printf(1, "TEST PASSED");
  exit();
}
