#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <assert.h>
#include <dirent.h>
#include <stdbool.h>
#include <sys/mman.h>

#define stat xv6_stat  // avoid clash with host struct stat
#define dirent xv6_dirent  // avoid clash with host struct stat
#include "types.h"
#include "fs.h"
#undef stat
#undef dirent



int main_checker(int fd){

  struct stat sbuf;
  fstat(fd, &sbuf);

  void *img_ptr = mmap(NULL,sbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

  // -----------------------CHECK 1-----------------------

  struct superblock *sb = (struct superblock *) (img_ptr + BSIZE);

  int file_system_size = sb->size;
  int num_data_blocks = sb->nblocks;
  int num_inodes = sb->ninodes;
  int num_inode_blocks = num_inodes / IPB;
  int last_bit_block_location = BBLOCK((file_system_size-1),num_inodes);

  if(file_system_size <= last_bit_block_location+num_data_blocks){
    fprintf(stderr,"ERROR: superblock is corrupted.\n");
    return 1;
  }

  // -----------------------CHECK 2-----------------------

  struct dinode *dip = (struct dinode *) (img_ptr + 2*BSIZE);

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type<0 || dip[i].type>3){
      fprintf(stderr,"ERROR: bad inode.\n");
      return 1;
    }
  }

  // -----------------------CHECK 3-----------------------

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type!=0){
      for(int j=0;j<NDIRECT;j++){
        int addr = dip[i].addrs[j];
        if(addr!=0 && (addr <= last_bit_block_location || addr >= file_system_size)){
          fprintf(stderr,"ERROR: bad direct address in inode.\n");
          return 1;
        }
      }
    }
  }

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type!=0){
      int addr = dip[i].addrs[NDIRECT];

      if(addr!=0){
        if(addr <= last_bit_block_location || addr >= file_system_size){
          fprintf(stderr,"ERROR: bad indirect address in inode.\n");
          return 1;
        }
        uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
        for (int j = 0; j < BSIZE/sizeof(uint); ++j) {
          int iaddr = indirect_addr[j];
          if(iaddr!=0 && (iaddr <= last_bit_block_location || iaddr >= file_system_size)){
            fprintf(stderr,"ERROR: bad indirect address in inode.\n");
            return 1;
          }
        }
      }

    }
  }

  // -----------------------CHECK 4-----------------------

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type==1){
      struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + dip[i].addrs[0] * BSIZE);
      if(strcmp(dent[0].name, ".")!=0 || strcmp(dent[1].name, "..")!=0 || dent[0].inum!=i){
        fprintf(stderr,"ERROR: directory not properly formatted.\n");
        return 1;
      }
    }
  }

  // -----------------------CHECK 5-----------------------

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type!=0){
      for(int j=0;j<NDIRECT;j++){
        int addr = dip[i].addrs[j];
        if(addr!=0){
          int bit_block_num = BBLOCK(addr,num_inodes);
          int bit_location = addr % BPB;
          uchar* bit_buf = (uchar*) (img_ptr + BSIZE * bit_block_num);
          if((bit_buf[bit_location/8] & 1) == 0){
            fprintf(stderr,"ERROR: address used by inode but marked free in bitmap.\n");
            return 1;
          }
        }
      }
    }
  }

  for(int i=0;i<num_inodes;i++){
    if(dip[i].type!=0){
      int addr = dip[i].addrs[NDIRECT];

      if(addr!=0){
        if(addr <= last_bit_block_location || addr >= file_system_size){
          fprintf(stderr,"ERROR: bad indirect address in inode.\n");
          return 1;
        }
        uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
        for (int j = 0; j < BSIZE/sizeof(uint); ++j) {
          int iaddr = indirect_addr[j];
          if(iaddr!=0){
            int bit_block_num = BBLOCK(iaddr,num_inodes);
            int bit_location = iaddr % BPB;
            uchar* bit_buf = (uchar*) (img_ptr + BSIZE * bit_block_num);
            if((bit_buf[bit_location/8] & 1) == 0){
              fprintf(stderr,"ERROR: address used by inode but marked free in bitmap.\n");
              return 1;
            }
          }
        }
      }

    }
  }




  return 0;
}

int main(int argc, char const *argv[]) {
  if(argc!=2){
    fprintf(stderr,"Usage: xfsck <file_system_image>");
    exit(1);
  }

  int fd = open(argv[1], O_RDONLY);

  if(fd<0){
    fprintf(stderr,"image not found.");
    exit(1);
  }

  if(main_checker(fd) == 1){
    exit(1);
  }
  else{
    exit(0);
  }

}
