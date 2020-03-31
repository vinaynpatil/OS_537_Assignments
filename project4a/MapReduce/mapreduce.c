#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <mapreduce.h>

void MR_EmitToCombiner(char *key, char *value)
{
}
void MR_EmitToReducer(char *key, char *value)
{
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Combiner combine,
            Partitioner partition)
{
}