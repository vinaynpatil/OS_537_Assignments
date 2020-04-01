#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_EmitToCombiner(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Combine(char *key, CombineGetter get_next) {
    int count = 0;
    char *value;
    while ((value = get_next(key)) != NULL) {
        count ++; // Emmited Map values are "1"s
    }
    // Convert integer (count) to string (value)
    value = (char*)malloc(10 * sizeof(char));
    // sprintf(value, "%d", count);
    printf("%d", count);
    MR_EmitToReducer(key, value);
    free(value);
}

void Reduce(char *key, ReduceStateGetter get_state,
            ReduceGetter get_next, int partition_number) {
    // `get_state` is only being used for "eager mode" (explained later)
    assert(get_state == NULL);

    int count = 0;

    char *value;
    while ((value = get_next(key, partition_number)) != NULL) {
        count += atoi(value);
    }

    // Convert integer (count) to string (value)
    value = (char*)malloc(10 * sizeof(char));
    sprintf(value, "%d", count);

    printf("%s %s\n", key, value);
    free(value);
}

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 5,
        Reduce, 10, Combine, MR_DefaultHashPartition);
}