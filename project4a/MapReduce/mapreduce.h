#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*CombineGetter)(char *key);
typedef char *(*ReduceGetter)(char *key, int partition_number);
typedef char *(*ReduceStateGetter)(char* key, int partition_number);

typedef void (*Mapper)(char *file_name);
typedef void (*Combiner)(char *key, CombineGetter get_next);
// Simple mode: `get_state` is NULL and `get_next` can be called until you get NULL
// Eager mode: `get_state` and `get_next` will only be called once inside the reducer
// More details on the modes are provided later
typedef void (*Reducer)(char *key, ReduceStateGetter get_state,
                        ReduceGetter get_next, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what *you must implement*
void MR_EmitToCombiner(char *key, char *value);
void MR_EmitToReducer(char *key, char *value);
// NOTE: Needs to be implemented ONLY for the Eager mode
void MR_EmitReducerState(char* key, char* state, int partition_number);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition);

#endif // __mapreduce_h__