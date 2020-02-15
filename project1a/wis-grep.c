#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char const *argv[])
{
    if (argc == 1)
    {
        printf("%s", "wis-grep: searchterm [file …]\n");
        exit(1);
    }

    else if (argc == 2)
    {
        char *line = NULL;
        size_t size = 0;
        while (getline(&line, &size, stdin) != -1)
        {
            if (strstr(line, argv[1]) != NULL)
            {
                printf("%s", line);
            }
        }
        exit(0);
    }

    else
    {
        FILE *FP;

        for (int i = 0; i < argc - 2; i++)
        {
            FP = fopen(argv[2 + i], "r");

            if (FP == NULL)
            {
                printf("%s", "wis-grep: cannot open file\n");
                exit(1);
            }

            char *line = NULL;
            size_t size = 0;

            while (getline(&line, &size, FP) != -1)
            {
                if (strstr(line, argv[1]) != NULL)
                {
                    printf("%s", line);
                }
            }

            fclose(FP);
        }

        exit(0);
    }

    return 0;
}
