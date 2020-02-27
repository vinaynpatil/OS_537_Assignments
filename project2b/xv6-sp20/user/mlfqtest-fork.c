#include "types.h"

#include "stat.h"

#include "user.h"

#include "pstat.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    printf(1, "usage: mlfqtest-fork counter");
    exit();
  }

  struct pstat st;
  int pid;
  int fst;

  int x = 0;
  fst = fork();

  // You spin my head right round, right round
  for (int i = 1; i < atoi(argv[1]); i++) {
    x = x + i;
  }

  getprocinfo(&st);
  pid = getpid();
  for (int pindex = 0; pindex < NPROC; pindex++) {
    if (st.pid[pindex] == pid) {
      printf(1, "PID: %d, { P%d:%d, P%d:%d, P%d:%d, P%d:%d }\n", pid, 3,
             st.ticks[pindex][3], 2, st.ticks[pindex][2], 1,
             st.ticks[pindex][1], 0, st.ticks[pindex][0]);
    }
  }

  if (fst > 0) {
    wait();
  }

  exit();
  return 0;
}