#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

int main(int argc, char const *argv[])
{
    if (argc <= 2)
    {
        printf("%s", "wis-tar: tar-file file […]\n");
        exit(1);
    }

    else
    {
        FILE *tar_file = fopen(argv[1], "w");

        FILE *FP;

        for (int i = 0; i < argc - 2; i++)
        {
            FP = fopen(argv[2 + i], "r");

            if (FP == NULL)
            {
                printf("%s", "wis-tar: cannot open file\n");
                exit(1);
            }

            struct stat info;
            int err = stat(argv[2 + i], &info);
            if (err != 0)
            {
                printf("%s", "Error occured while reading stat info");
            }

            char filename[100] = {'\0'};

            long filesize = info.st_size;

            int num_elements_to_copy = 100;

            if (strlen(argv[2 + i]) < num_elements_to_copy)
            {
                num_elements_to_copy = strlen(argv[2 + i]);
            }

            strncpy(filename, argv[2 + i], num_elements_to_copy);

            fwrite(filename, 1, sizeof(filename), tar_file);

            fwrite(&filesize, 1, sizeof(long), tar_file);

            char *line = NULL;
            size_t size = 0;

            while (getline(&line, &size, FP) != -1)
            {
                fwrite(line, 1, strlen(line), tar_file);
            }

            fclose(FP);
        }

        fclose(tar_file);

        exit(0);
    }

    return 0;
}
