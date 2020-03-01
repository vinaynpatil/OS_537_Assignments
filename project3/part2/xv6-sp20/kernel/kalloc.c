// Physical memory allocator, intended to allocate
// memory for user processes, kernel stacks, page table pages,
// and pipe buffers. Allocates 4096-byte pages.

#include "types.h"
#include "defs.h"
#include "param.h"
#include "mmu.h"
#include "spinlock.h"
#include "rand.c"

int current_count =0;

int frames_history[512];
int frames_index = 0;

struct run {
  struct run *next;
};

struct {
  struct spinlock lock;
  struct run *freelist;
} kmem;

extern char end[]; // first address after kernel loaded from ELF file

int size_of_free_list = 0;

// Initialize free list of physical pages.
void
kinit(void)
{
  char *p;

  initlock(&kmem.lock, "kmem");
  p = (char*)PGROUNDUP((uint)end);
  for(; p + PGSIZE <= (char*)PHYSTOP; p += PGSIZE)
    kfree(p);
}

// Free the page of physical memory pointed at by v,
// which normally should have been returned by a
// call to kalloc().  (The exception is when
// initializing the allocator; see kinit above.)
void
kfree(char *v)
{
  struct run *r;

  if((uint)v % PGSIZE || v < end || (uint)v >= PHYSTOP) 
    panic("kfree");

  // Fill with junk to catch dangling refs.
  memset(v, 1, PGSIZE);

  acquire(&kmem.lock);
  r = (struct run*)v;
  r->next = kmem.freelist;
  kmem.freelist = r;

  size_of_free_list++;

  release(&kmem.lock);
}

// Allocate one 4096-byte page of physical memory.
// Returns a pointer that the kernel can use.
// Returns 0 if the memory cannot be allocated.
char*
kalloc(void)
{
  struct run *r;
  struct run *prev;

  int random_number;
  int index;
  int i=0;

  acquire(&kmem.lock);
  r = kmem.freelist;
  if(r){
    random_number = xv6_rand();
    index = random_number % size_of_free_list;
    
    if(index==0){
      kmem.freelist = r->next;
    }
    else{
      while(i<index){
        prev = r;
        r = r->next;
        i++;
      }
      prev->next = r->next;
    }
  }
  else{
    release(&kmem.lock);
    return NULL;  
  }
  release(&kmem.lock);

  size_of_free_list--;
  
  if(r!=NULL){
    frames_history[frames_index] = (uint)r;
    frames_index++;
  }

  return (char*)r;
}


int dump_allocated_helper(int *frames, int numframes) {
  if(numframes>frames_index){
    return -1;
  }

  int index = 0;

  for(int i = frames_index-1;i>=(frames_index-numframes);i--){
    frames[index] = frames_history[i];;
    index++;
  }

  return 0;
}

