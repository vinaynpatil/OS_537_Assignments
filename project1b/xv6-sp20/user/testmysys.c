#include "types.h"
#include "stat.h"
#include "user.h"
#include "fs.h"
#include "fcntl.h"

int
main(int argc, char *argv[])
{
    int my_pid = getpid();
    int num_files1 = getfilenum(my_pid);
    printf(1,"The no of open files are %d\n", num_files1);
   
    int fd;
    
    char path[] = "dummyfile.txt";

    fd = open(path, O_CREATE | O_RDWR);

    printf(1,"fd is %d\n",fd); 

    int num_files2 = getfilenum(my_pid);
    printf(1,"The no of open files are %d\n", num_files2);
    close(fd);

    exit();
}
