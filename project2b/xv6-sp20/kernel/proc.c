#include "types.h"
#include "defs.h"
#include "param.h"
#include "mmu.h"
#include "x86.h"
#include "proc.h"
#include "spinlock.h"
#include "pstat.h"

struct {
  struct spinlock lock;
  struct proc proc[NPROC];
} ptable;

static struct proc *initproc;

int queue_time_slice[4] = {-1, 32, 16, 8}; //-1 denotes executes the process until completion.

int round_robin_slice[4] = {64, 4, 2, 1}; 

int queue_index[4] = {-1, -1, -1, -1};

int wait_max[4] = {640, 320, 160, 80};

int QUEUE3 = 3;
int QUEUE2 = 2;
int QUEUE1 = 1;
int QUEUE0 = 0;


int queue_size = 10000;

struct proc* schedular_queue[4][10000];

int new_process_in_queue_3 = 0;

int nextpid = 1;
extern void forkret(void);
extern void trapret(void);

static void wakeup1(void *chan);

void
pinit(void)
{
  initlock(&ptable.lock, "ptable");
}

// Look in the process table for an UNUSED proc.
// If found, change state to EMBRYO and initialize
// state required to run in the kernel.
// Otherwise return 0.
static struct proc*
allocproc(void)
{
  struct proc *p;
  char *sp;

  acquire(&ptable.lock);
  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++)
    if(p->state == UNUSED)
      goto found;

  release(&ptable.lock);
  return 0;

found:
  p->state = EMBRYO;
  p->pid = nextpid++;


  p->queue_priority = 3;
  
  p->ticks_in_each_level[0] = 0;
  p->ticks_in_each_level[1] = 0;
  p->ticks_in_each_level[2] = 0;
  p->ticks_in_each_level[3] = 0;


  p->wait_ticks_in_each_level[0] = 0;
  p->wait_ticks_in_each_level[1] = 0;
  p->wait_ticks_in_each_level[2] = 0;
  p->wait_ticks_in_each_level[3] = 0;

  p->round_robin_counter = 0;

  queue_index[QUEUE3] = queue_index[QUEUE3] + 1;
	schedular_queue[QUEUE3][queue_index[QUEUE3]] = p;

  new_process_in_queue_3 = 1;

  release(&ptable.lock);

  // Allocate kernel stack if possible.
  if((p->kstack = kalloc()) == 0){
    p->state = UNUSED;
    return 0;
  }
  sp = p->kstack + KSTACKSIZE;
  
  // Leave room for trap frame.
  sp -= sizeof *p->tf;
  p->tf = (struct trapframe*)sp;
  
  // Set up new context to start executing at forkret,
  // which returns to trapret.
  sp -= 4;
  *(uint*)sp = (uint)trapret;

  sp -= sizeof *p->context;
  p->context = (struct context*)sp;
  memset(p->context, 0, sizeof *p->context);
  p->context->eip = (uint)forkret;

  
  
  return p;
}

// Set up first user process.
void
userinit(void)
{
  struct proc *p;
  extern char _binary_initcode_start[], _binary_initcode_size[];
  
  p = allocproc();
  acquire(&ptable.lock);
  initproc = p;
  if((p->pgdir = setupkvm()) == 0)
    panic("userinit: out of memory?");
  inituvm(p->pgdir, _binary_initcode_start, (int)_binary_initcode_size);
  p->sz = PGSIZE;
  memset(p->tf, 0, sizeof(*p->tf));
  p->tf->cs = (SEG_UCODE << 3) | DPL_USER;
  p->tf->ds = (SEG_UDATA << 3) | DPL_USER;
  p->tf->es = p->tf->ds;
  p->tf->ss = p->tf->ds;
  p->tf->eflags = FL_IF;
  p->tf->esp = PGSIZE;
  p->tf->eip = 0;  // beginning of initcode.S

  safestrcpy(p->name, "initcode", sizeof(p->name));
  p->cwd = namei("/");

  p->state = RUNNABLE;
  release(&ptable.lock);
}

// Grow current process's memory by n bytes.
// Return 0 on success, -1 on failure.
int
growproc(int n)
{
  uint sz;
  
  sz = proc->sz;
  if(n > 0){
    if((sz = allocuvm(proc->pgdir, sz, sz + n)) == 0)
      return -1;
  } else if(n < 0){
    if((sz = deallocuvm(proc->pgdir, sz, sz + n)) == 0)
      return -1;
  }
  proc->sz = sz;
  switchuvm(proc);
  return 0;
}

// Create a new process copying p as the parent.
// Sets up stack to return as if from system call.
// Caller must set state of returned proc to RUNNABLE.
int
fork(void)
{
  int i, pid;
  struct proc *np;

  // Allocate process.
  if((np = allocproc()) == 0)
    return -1;

  // Copy process state from p.
  if((np->pgdir = copyuvm(proc->pgdir, proc->sz)) == 0){
    kfree(np->kstack);
    np->kstack = 0;
    np->state = UNUSED;
    return -1;
  }
  np->sz = proc->sz;
  np->parent = proc;
  *np->tf = *proc->tf;

  // Clear %eax so that fork returns 0 in the child.
  np->tf->eax = 0;

  for(i = 0; i < NOFILE; i++)
    if(proc->ofile[i])
      np->ofile[i] = filedup(proc->ofile[i]);
  np->cwd = idup(proc->cwd);
 
  pid = np->pid;
  np->state = RUNNABLE;
  safestrcpy(np->name, proc->name, sizeof(proc->name));
  return pid;
}

// Exit the current process.  Does not return.
// An exited process remains in the zombie state
// until its parent calls wait() to find out it exited.
void
exit(void)
{
  struct proc *p;
  int fd;

  if(proc == initproc)
    panic("init exiting");

  // Close all open files.
  for(fd = 0; fd < NOFILE; fd++){
    if(proc->ofile[fd]){
      fileclose(proc->ofile[fd]);
      proc->ofile[fd] = 0;
    }
  }

  iput(proc->cwd);
  proc->cwd = 0;

  acquire(&ptable.lock);

  // Parent might be sleeping in wait().
  wakeup1(proc->parent);

  // Pass abandoned children to init.
  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++){
    if(p->parent == proc){
      p->parent = initproc;
      if(p->state == ZOMBIE)
        wakeup1(initproc);
    }
  }

  // Jump into the scheduler, never to return.
  proc->state = ZOMBIE;
  sched();
  panic("zombie exit");
}

// Wait for a child process to exit and return its pid.
// Return -1 if this process has no children.
int
wait(void)
{
  struct proc *p;
  int havekids, pid;

  acquire(&ptable.lock);
  for(;;){
    // Scan through table looking for zombie children.
    havekids = 0;
    for(p = ptable.proc; p < &ptable.proc[NPROC]; p++){
      if(p->parent != proc)
        continue;
      havekids = 1;
      if(p->state == ZOMBIE){
        // Found one.
        pid = p->pid;
        kfree(p->kstack);
        p->kstack = 0;
        freevm(p->pgdir);
        p->state = UNUSED;
        p->pid = 0;
        p->parent = 0;
        p->name[0] = 0;
        p->killed = 0;
        release(&ptable.lock);
        return pid;
      }
    }

    // No point waiting if we don't have any children.
    if(!havekids || proc->killed){
      release(&ptable.lock);
      return -1;
    }

    // Wait for children to exit.  (See wakeup1 call in proc_exit.)
    sleep(proc, &ptable.lock);  //DOC: wait-sleep
  }
}

// Per-CPU process scheduler.
// Each CPU calls scheduler() after setting itself up.
// Scheduler never returns.  It loops, doing:
//  - choose a process to run
//  - swtch to start running that process
//  - eventually that process transfers control
//      via swtch back to the scheduler.
void
scheduler(void)
{
  struct proc *p;

  for(;;){
    // Enable interrupts on this processor.
    sti();

    // Loop over process table looking for process to run.
    acquire(&ptable.lock);


    int queueIterator = 3;
    
    while(queueIterator>=0){
	   
      int index = 0;
      //int round_robin_counter = 0; 
      while(index<=queue_index[queueIterator]){

        if(queueIterator!=3){
          if(new_process_in_queue_3 == 1){
            new_process_in_queue_3 = 0;
            queueIterator = 3;
            break; //Start again from the high priority queue.
          }
        }
        else{
          new_process_in_queue_3 = 0;
        }

        p = schedular_queue[queueIterator][index];

        if(p->state != RUNNABLE)
        {
          index++;
          continue;
        }
        p->round_robin_counter++;

        p->ticks_in_each_level[queueIterator]++;

        p->wait_ticks_in_each_level[queueIterator] = 0;
        
        // Switch to chosen process.  It is the process's job
        // to release ptable.lock and then reacquire it
        // before jumping back to us.
        proc = p;
        switchuvm(p);
        p->state = RUNNING;
        swtch(&cpu->scheduler, proc->context);
        switchkvm();

        
	// if((p->state==RUNNABLE) && p->round_robin_counter<=round_robin_slice[queueIterator]){
	  // index--;
	// }
	// index++;
	

	
        // If a process exceeds it's time slice in a queue other than the 0 level queue then demote it.
	 if((queue_time_slice[queueIterator]!=-1) && (p->ticks_in_each_level[queueIterator]==queue_time_slice[queueIterator])){
          
          schedular_queue[queueIterator][index]=NULL;
	  
          for(int i=index; i<queue_index[queueIterator]; i++){
            schedular_queue[queueIterator][i] = schedular_queue[queueIterator][i+1];
          }
						
          schedular_queue[queueIterator][queue_index[queueIterator]] = NULL;

          queue_index[queueIterator] = queue_index[queueIterator] -1;

	  
          queue_index[queueIterator-1] = queue_index[queueIterator-1] + 1;
	  
          p->queue_priority--;
	  
	  
	  p->round_robin_counter = 0;
	  
	  //p->ticks_in_each_level[queueIterator] = 0;
	  if(p->ticks_in_each_level[queueIterator-1]==queue_time_slice[queueIterator-1]){
            p->ticks_in_each_level[queueIterator-1] = 0;
          }
	  
          schedular_queue[queueIterator-1][queue_index[queueIterator-1]] = p; //Demoting the process to a lower level queue.
	 
          // index++; //Because of the left movement, new process takes the current process position.
        }


	else if((p->state==RUNNABLE) && p->round_robin_counter==round_robin_slice[queueIterator]){
         
	  p->round_robin_counter = 0;

          schedular_queue[queueIterator][index]=NULL;

          for(int i=index; i<queue_index[queueIterator]; i++){
            schedular_queue[queueIterator][i] = schedular_queue[queueIterator][i+1];
          }
						
	  schedular_queue[queueIterator][queue_index[queueIterator]] = p;
	  
	  //p->round_robin_counter = 0;

          // index++; //Because of the left movement, new process takes the current process position.
        }




        // Increment wait time for each of the processes and boost them if the criteria is met.
        int queues_counter = 3;
        
        int switch_to_queue = queueIterator;
        while(queues_counter>=0){
		
          int iter = 0;
          while(iter<=queue_index[queues_counter]){
		  
           if((schedular_queue[queues_counter][iter]->pid!=p->pid) && schedular_queue[queues_counter][iter]->state == RUNNABLE){
            schedular_queue[queues_counter][iter]->wait_ticks_in_each_level[queues_counter]++;
            
            //Boosting the priority for the processes who have exceeded their max wait time in the queue.
            if(queues_counter!=3 && (schedular_queue[queues_counter][iter]->wait_ticks_in_each_level[queues_counter])==wait_max[queues_counter]){
              schedular_queue[queues_counter][iter]->queue_priority++; 
              
              schedular_queue[queues_counter][iter]->wait_ticks_in_each_level[queues_counter] = 0;//Resetting the wait for the old level.
	      schedular_queue[queues_counter][iter]->wait_ticks_in_each_level[queues_counter+1] = 0;
              //Move the process to a higher level.
              struct proc *temp = schedular_queue[queues_counter][iter];

              schedular_queue[queues_counter][iter]=NULL;

              for(int i=iter; i<queue_index[queues_counter]; i++){
                schedular_queue[queues_counter][i] = schedular_queue[queues_counter][i+1];
              }
                
              schedular_queue[queues_counter][queue_index[queues_counter]] = NULL;

              queue_index[queues_counter]--;


              queue_index[queues_counter+1]++;

              if(temp->ticks_in_each_level[queues_counter+1]==queue_time_slice[queues_counter+1]){
            	temp->ticks_in_each_level[queues_counter+1] = 0;
              }

	      if(temp->ticks_in_each_level[queues_counter]==queue_time_slice[queues_counter]){
                temp->ticks_in_each_level[queues_counter] = 0;
              }

              if(temp->queue_priority > switch_to_queue){
                switch_to_queue = temp->queue_priority;
              }

              temp->round_robin_counter = 0;

              schedular_queue[queues_counter+1][queue_index[queues_counter+1]] = temp;

              iter--;

            }
	    }
	   

            iter++;

          }

          queues_counter--;
	 

        }

	


        // Check if there is a new process in a higher priority queue i.e. a process whose priority is greater than queueIterator.
        // Break the while loop and set queueIterator to the higher queue with atleast one process in it.

        // Process is done running for now.
        // It should have changed its p->state before coming back.
        proc = 0;

	if(switch_to_queue > queueIterator){
          queueIterator = switch_to_queue;
          break;
        }

	if((p->state==RUNNABLE) && p->round_robin_counter<=round_robin_slice[queueIterator]){
           index--;
         }
         index++;
        //index = index + 1;
      }

      

      queueIterator = queueIterator - 1;

    }

    release(&ptable.lock);

  }
}

// Enter scheduler.  Must hold only ptable.lock
// and have changed proc->state.
void
sched(void)
{
  int intena;

  if(!holding(&ptable.lock))
    panic("sched ptable.lock");
  if(cpu->ncli != 1)
    panic("sched locks");
  if(proc->state == RUNNING)
    panic("sched running");
  if(readeflags()&FL_IF)
    panic("sched interruptible");
  intena = cpu->intena;
  swtch(&proc->context, cpu->scheduler);
  cpu->intena = intena;
}

// Give up the CPU for one scheduling round.
void
yield(void)
{
  acquire(&ptable.lock);  //DOC: yieldlock
  proc->state = RUNNABLE;
  sched();
  release(&ptable.lock);
}

// A fork child's very first scheduling by scheduler()
// will swtch here.  "Return" to user space.
void
forkret(void)
{
  // Still holding ptable.lock from scheduler.
  release(&ptable.lock);
  
  // Return to "caller", actually trapret (see allocproc).
}

// Atomically release lock and sleep on chan.
// Reacquires lock when awakened.
void
sleep(void *chan, struct spinlock *lk)
{
  if(proc == 0)
    panic("sleep");

  if(lk == 0)
    panic("sleep without lk");

  // Must acquire ptable.lock in order to
  // change p->state and then call sched.
  // Once we hold ptable.lock, we can be
  // guaranteed that we won't miss any wakeup
  // (wakeup runs with ptable.lock locked),
  // so it's okay to release lk.
  if(lk != &ptable.lock){  //DOC: sleeplock0
    acquire(&ptable.lock);  //DOC: sleeplock1
    release(lk);
  }

  // Go to sleep.
  proc->chan = chan;
  proc->state = SLEEPING;
  sched();

  // Tidy up.
  proc->chan = 0;

  // Reacquire original lock.
  if(lk != &ptable.lock){  //DOC: sleeplock2
    release(&ptable.lock);
    acquire(lk);
  }
}

// Wake up all processes sleeping on chan.
// The ptable lock must be held.
static void
wakeup1(void *chan)
{
  struct proc *p;

  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++)
    if(p->state == SLEEPING && p->chan == chan)
      p->state = RUNNABLE;
}

// Wake up all processes sleeping on chan.
void
wakeup(void *chan)
{
  acquire(&ptable.lock);
  wakeup1(chan);
  release(&ptable.lock);
}

// Kill the process with the given pid.
// Process won't exit until it returns
// to user space (see trap in trap.c).
int
kill(int pid)
{
  struct proc *p;

  acquire(&ptable.lock);
  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++){
    if(p->pid == pid){
      p->killed = 1;
      // Wake process from sleep if necessary.
      if(p->state == SLEEPING)
        p->state = RUNNABLE;
      release(&ptable.lock);
      return 0;
    }
  }
  release(&ptable.lock);
  return -1;
}

// Print a process listing to console.  For debugging.
// Runs when user types ^P on console.
// No lock to avoid wedging a stuck machine further.
void
procdump(void)
{
  static char *states[] = {
  [UNUSED]    "unused",
  [EMBRYO]    "embryo",
  [SLEEPING]  "sleep ",
  [RUNNABLE]  "runble",
  [RUNNING]   "run   ",
  [ZOMBIE]    "zombie"
  };
  int i;
  struct proc *p;
  char *state;
  uint pc[10];
  
  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++){
    if(p->state == UNUSED)
      continue;
    if(p->state >= 0 && p->state < NELEM(states) && states[p->state])
      state = states[p->state];
    else
      state = "???";
    cprintf("%d %s %s", p->pid, state, p->name);
    if(p->state == SLEEPING){
      getcallerpcs((uint*)p->context->ebp+2, pc);
      for(i=0; i<10 && pc[i] != 0; i++)
        cprintf(" %p", pc[i]);
    }
    cprintf("\n");
  }
}

int getprocinfo(struct pstat * stats){
  
  int index = 0;
  struct proc *p;

  for(p = ptable.proc; p < &ptable.proc[NPROC]; p++){
    if(p->state== UNUSED)
      stats->inuse[index] = 0;
    else{
      stats->inuse[index] = 1;
    }

    stats->pid[index] = p->pid;
    stats->priority[index] = p->queue_priority;
    stats->state[index] = p->state;
    
    stats->ticks[index][0] = p->ticks_in_each_level[0];
    stats->ticks[index][1] = p->ticks_in_each_level[1];
    stats->ticks[index][2] = p->ticks_in_each_level[2];
    stats->ticks[index][3] = p->ticks_in_each_level[3];

    stats->wait_ticks[index][0] = p->wait_ticks_in_each_level[0];
    stats->wait_ticks[index][1] = p->wait_ticks_in_each_level[1];
    stats->wait_ticks[index][2] = p->wait_ticks_in_each_level[2];
    stats->wait_ticks[index][3] = p->wait_ticks_in_each_level[3];

    index = index + 1;
  }

	return 0; //Success.
}


int boostproc(void){

  if(proc->queue_priority!=3){

    struct proc *p1;

    int queue_number = proc->queue_priority;

    for(int i=0;i<queue_size;i++){

      p1 = schedular_queue[queue_number][i];

      if(p1->pid == proc->pid){
        schedular_queue[queue_number][i]->wait_ticks_in_each_level[queue_number] = 0;
	schedular_queue[queue_number][i]->wait_ticks_in_each_level[queue_number+1] = 0;
        

	schedular_queue[queue_number][i] = NULL;
        for(int j=i; i<queue_index[queue_number]; j++){
          schedular_queue[queue_number][j] = schedular_queue[queue_number][j+1];
        }
        schedular_queue[queue_number][queue_index[queue_number]] = NULL;

        queue_index[queue_number] = queue_index[queue_number] -1;

        queue_index[queue_number+1] = queue_index[queue_number+1] + 1;

        p1->queue_priority = p1->queue_priority + 1;

        p1->round_robin_counter = 0;

	if(p1->ticks_in_each_level[queue_number+1]==queue_time_slice[queue_number+1]){
	
		p1->ticks_in_each_level[queue_number+1] = 0;
	}

	if(p1->ticks_in_each_level[queue_number]==queue_time_slice[queue_number]){

                p1->ticks_in_each_level[queue_number] = 0;
        }

        schedular_queue[queue_number+1][queue_index[queue_number+1]] = p1;

        new_process_in_queue_3 = 1;

        return 0;

      }

    }


  }

  return 0;
}


