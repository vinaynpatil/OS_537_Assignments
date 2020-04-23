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
  int first_bit_block_location = BBLOCK(0,num_inodes);
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
/*
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
*/
  // -----------------------CHECK 6-----------------------
/*

for(int i=first_bit_block_location;i<=last_bit_block_location;i++){
  uchar* bit_buf = (uchar*) (img_ptr + BSIZE * i);
  int j=0;
  if(i==first_bit_block_location){
    j = first_bit_block_location + 1;
  }
  for(; j < BSIZE*8; j++){
    if((bit_buf[j/8] & 1) == 1){

      int addr_to_check = j + ((BSIZE*8)*(last_bit_block_location - i));

      int flag = 0;

      for(int k=0;k<num_inodes;k++){
        if(dip[k].type!=0){
          for(int h=0;h<NDIRECT;h++){
            int addr = dip[k].addrs[h];
            if(addr == addr_to_check){
              flag = 1;
              break;
            }
          }
        }
        if(flag == 1){
          break;
        }
      }

      for(int k=0;k<num_inodes;k++){
        if(dip[k].type!=0){
          int addr = dip[k].addrs[NDIRECT];
          if(addr!=0){
            uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
            for (int h = 0; h < BSIZE/sizeof(uint); ++h) {
              int iaddr = indirect_addr[h];
              if(iaddr == addr_to_check){
                flag = 1;
                break;
              }
            }
          }
        }
        if(flag==1){
          break;
        }
      }

      if(flag==0){
        printf("%d\n", addr_to_check );
        fprintf(stderr,"ERROR: bitmap marks block in use but it is not in use.\n");
        return 1;
      }

    }
  }
}

*/
// -----------------------CHECK 7-----------------------

int *direct_addr_array = (int *) malloc(file_system_size * sizeof(int));

for(int i=0;i<num_inodes;i++){
  if(dip[i].type!=0){
    for(int j=0;j<NDIRECT;j++){
      int addr = dip[i].addrs[j];
      if(addr!=0){
        if(direct_addr_array[addr] != 0){
          fprintf(stderr,"ERROR: direct address used more than once.\n");
          return 1;
        }
        direct_addr_array[addr]++;
      }
    }
  }
}

// -----------------------CHECK 8-----------------------

for(int i=0;i<num_inodes;i++){
  if(dip[i].type == 2){
    int blocks_used = 0;
    for(int j=0;j<NDIRECT;j++){
      if(dip[i].addrs[j]!=0){
        blocks_used++;
      }
    }

    int addr = dip[i].addrs[NDIRECT];

    if(addr!=0){
      uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
      for (int j = 0; j < BSIZE/sizeof(uint); ++j) {
        if(indirect_addr[j]!=0){
          blocks_used++;
        }
      }
    }

    int size =  (int)dip[i].size;

    if(!(((blocks_used-1)*BSIZE < size) && (size <= blocks_used*BSIZE))){
      fprintf(stderr, "ERROR: incorrect file size in inode.\n");
      return 1;
    }

  }
}

// -----------------------CHECK 9-----------------------

for(int i=0;i<num_inodes;i++){
  if(dip[i].type!=0){
    int flag = 0;

    for(int j=0;j<num_inodes;j++){
      if(dip[j].type==1){
        for(int k=0;k<NDIRECT;k++){
          if(dip[j].addrs[k]!=0){

            struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + dip[j].addrs[k] * BSIZE);

            for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
              if(strcmp(dent[l].name, ".")!=0 && dent[l].inum==i){
                flag = 1;
                break;
              }
            }
          }
          if(flag==1){
            break;
          }
        }

        if(flag==1){
          break;
        }

        int addr = dip[j].addrs[NDIRECT];

        if(addr!=0){
          uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
          for (int k = 0; k < BSIZE/sizeof(uint); ++k) {
            if(indirect_addr[k]!=0){

              struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + indirect_addr[k] * BSIZE);

              for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
                if(strcmp(dent[l].name, ".")!=0 && dent[l].inum==i){
                  flag = 1;
                  break;
                }
              }
            }
            if(flag==1){
              break;
            }
          }
        }
      }
      if(flag==1){
        break;
      }
    }

    if(flag==0){
      fprintf(stderr,"ERROR: inode marked used but not found in a directory.\n");
      return 1;
    }

  }
}


// -----------------------CHECK 10-----------------------


    for(int j=0;j<num_inodes;j++){
      if(dip[j].type==1){
        for(int k=0;k<NDIRECT;k++){
          if(dip[j].addrs[k]!=0){
            struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + dip[j].addrs[k] * BSIZE);
            for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
              if(dent[l].inum!=0 && dip[dent[l].inum].type == 0){
                fprintf(stderr,"ERROR: inode referred to in directory but marked free.\n");
                return 1;
              }
            }
          }
        }


        int addr = dip[j].addrs[NDIRECT];

        if(addr!=0){
          uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
          for (int k = 0; k < BSIZE/sizeof(uint); ++k) {
            if(indirect_addr[k]!=0){

              struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + indirect_addr[k] * BSIZE);
              for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
                if(dent[l].inum!=0 && dip[dent[l].inum].type == 0){
                  fprintf(stderr,"ERROR: inode referred to in directory but marked free.\n");
                  return 1;
                }
              }
            }
          }
        }
      }
    }

// -----------------------CHECK 11-----------------------

for(int i=0;i<num_inodes;i++){
  if(dip[i].type==2){
    int count_file_refs = 0;

    for(int j=0;j<num_inodes;j++){
      if(dip[j].type==1){
        for(int k=0;k<NDIRECT;k++){
          if(dip[j].addrs[k]!=0){

            struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + dip[j].addrs[k] * BSIZE);

            for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
              if(dent[l].inum==i){
                count_file_refs++;
              }
            }
          }
        }

        int addr = dip[j].addrs[NDIRECT];

        if(addr!=0){
          uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
          for (int k = 0; k < BSIZE/sizeof(uint); ++k) {
            if(indirect_addr[k]!=0){

              struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + indirect_addr[k] * BSIZE);

              for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
                if(dent[l].inum==i){
                  count_file_refs++;
                }
              }
            }
          }
        }
      }
    }

    if(count_file_refs!=dip[i].nlink){
      fprintf(stderr,"ERROR: bad reference count for file.\n");
      return 1;
    }
  }
}

// -----------------------CHECK 12-----------------------

for(int i=0;i<num_inodes;i++){
  if(dip[i].type==1){
    int count_dir_refs = 0;

    for(int j=0;j<num_inodes;j++){
      if(dip[j].type==1){
        for(int k=0;k<NDIRECT;k++){
          if(dip[j].addrs[k]!=0){

            struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + dip[j].addrs[k] * BSIZE);

            for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
              if(strcmp(dent[l].name,".")!=0 && strcmp(dent[l].name,"..")!=0 && dent[l].inum==i){
                count_dir_refs++;
              }
            }
          }
        }

        int addr = dip[j].addrs[NDIRECT];

        if(addr!=0){
          uint* indirect_addr = (uint*) (img_ptr + BSIZE * addr);
          for (int k = 0; k < BSIZE/sizeof(uint); ++k) {
            if(indirect_addr[k]!=0){

              struct xv6_dirent *dent = (struct xv6_dirent *) (img_ptr + indirect_addr[k] * BSIZE);

              for(int l=0;l<BSIZE / sizeof(struct xv6_dirent);l++){
                if(strcmp(dent[l].name,".")!=0 && strcmp(dent[l].name,"..")!=0 && dent[l].inum==i){
                  count_dir_refs++;
                }
              }
            }
          }
        }
      }
    }

    if(i==1 && count_dir_refs!=0){
      fprintf(stderr,"ERROR: directory appears more than once in file system.\n");
      return 1;
    }

    if(i!=1 && count_dir_refs!=1){
      fprintf(stderr,"ERROR: directory appears more than once in file system.\n");
      return 1;
    }

  }
}



  return 0;
}

int main(int argc, char const *argv[]) {
  if(argc!=2){
    fprintf(stderr,"Usage: xfsck <file_system_image>\n");
    exit(1);
  }

  int fd = open(argv[1], O_RDONLY);

  if(fd<0){
    fprintf(stderr,"image not found.\n");
    exit(1);
  }

  if(main_checker(fd) == 1){
    exit(1);
  }
  else{
    exit(0);
  }

}
