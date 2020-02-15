#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

int main(int argc, char const *argv[])
{
    if (argc <= 1 || argc >= 3)
    {
        printf("%s", "wis-untar: tar-file\n");
        exit(1);
    }

    else
    {
        FILE *tar_file = fopen(argv[1], "r");

        if (tar_file == NULL)
        {
            printf("%s", "wis-untar: cannot open file\n");
            exit(1);
        }

        char filename[100];

        while (fread(filename, 1, 100, tar_file) == 100)
        {

            long filesize = 0;

            fread(&filesize, 1, sizeof(long), tar_file);

	    long buffer = 1024;

	    FILE *newfile = fopen(filename, "w");
	    		

	    while (filesize>0){

		long temp = buffer;

		if(filesize<buffer){
	            temp = filesize;
		    filesize = 0;
		}
		else{
		    filesize = filesize - buffer;
		}		

            	char filecontent[temp];

            	fread(filecontent, 1, temp, tar_file);

            	fwrite(filecontent, 1, temp, newfile);
	    }
        }

        fclose(tar_file);

        exit(0);
    }

    return 0;
}
