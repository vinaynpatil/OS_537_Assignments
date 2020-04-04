#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

Partitioner partition_func;
Mapper map_func;
Reducer reduce_func;
Combiner combine_func;

pthread_t *all_mappers;
pthread_t *all_reducers;
pthread_mutex_t mapper_lock;

int mapper_count;
int reducer_count;

typedef char *string;

int getIndex(pthread_t thread_id)
{
    for (int i = 0; i < mapper_count; i++)
    {
        if (pthread_equal(thread_id, all_mappers[i]) != 0)
        {
            return i;
        }
    }
    return -1;
}

typedef struct chained_value
{
    struct chained_value *next;
    string linked_value;
} chained_value;

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

typedef struct bucket_chained_kv
{
    struct bucket_chained_kv *next;
    chained_value *value_container;
    string unique_key;
} bucket_chained_kv;

struct reducer_args
{
    int partition_number;
};

typedef struct bucket
{
    bucket_chained_kv *inner_heap;
} bucket;

int comparator(const void *first, const void *second)
{
    return strcmp(((bucket_chained_kv *)first)->unique_key, ((bucket_chained_kv *)second)->unique_key);
}

typedef struct kvtable
{
    bucket_chained_kv *all_keys;
    struct bucket *bucket_array;
    int unique_key_count;
    int tracker;
} kvtable;

int total_files;
kvtable *mapper_table;
const int table_buckets = 10000;
char **all_files;
kvtable *reducer_table;

void MR_EmitToCombiner(char *key, char *value)
{
    int thread_index = getIndex(pthread_self());
    unsigned long bucket_index = MR_DefaultHashPartition(key, table_buckets);
    bucket_chained_kv *heap = mapper_table[thread_index].bucket_array[bucket_index].inner_heap;

    while (heap != NULL && strcmp(key, heap->unique_key) != 0)
    {
        heap = heap->next;
    }

    chained_value *duplicate_value_node = malloc(sizeof(chained_value));
    duplicate_value_node->next = NULL;
    duplicate_value_node->linked_value = strdup(value);

    if (heap != NULL)
    {
        duplicate_value_node->next = heap->value_container;
        heap->value_container = duplicate_value_node;
    }
    else
    {
        mapper_table[thread_index].unique_key_count++;
        bucket_chained_kv *duplicate_key_node = malloc(sizeof(bucket_chained_kv));
        duplicate_key_node->next = mapper_table[thread_index].bucket_array[bucket_index].inner_heap;
        duplicate_key_node->unique_key = strdup(key);
        duplicate_key_node->value_container = duplicate_value_node;
        mapper_table[thread_index].bucket_array[bucket_index].inner_heap = duplicate_key_node;
    }
}

void MR_EmitToReducer(char *key, char *value)
{
    unsigned long reducer_bin = partition_func(key, reducer_count);
    unsigned long bucket_index = MR_DefaultHashPartition(key, table_buckets);
    bucket_chained_kv *heap = reducer_table[reducer_bin].bucket_array[bucket_index].inner_heap;

    while (heap != NULL && strcmp(key, heap->unique_key) != 0)
    {
        heap = heap->next;
    }

    chained_value *duplicate_value_node = malloc(sizeof(chained_value));
    duplicate_value_node->linked_value = strdup(value);
    duplicate_value_node->next = NULL;

    if (heap != NULL)
    {
        duplicate_value_node->next = heap->value_container;
        heap->value_container = duplicate_value_node;
    }
    else
    {
        reducer_table[reducer_bin].unique_key_count++;
        bucket_chained_kv *duplicate_key_node = malloc(sizeof(bucket_chained_kv));
        duplicate_key_node->next = reducer_table[reducer_bin].bucket_array[bucket_index].inner_heap;
        duplicate_key_node->unique_key = strdup(key);
        duplicate_key_node->value_container = duplicate_value_node;
        reducer_table[reducer_bin].bucket_array[bucket_index].inner_heap = duplicate_key_node;
    }
}

char *combine_get_next(char *key)
{
    int thread_index = getIndex(pthread_self());
    bucket_chained_kv *each_key = mapper_table[thread_index].all_keys;
    while (1)
    {
        int track_index = mapper_table[thread_index].tracker;
        if (strcmp(key, each_key[track_index].unique_key) != 0)
        {
            mapper_table[thread_index].tracker++;
        }
        else
        {
            if (each_key[track_index].value_container == NULL)
                return NULL;

            string value = each_key[track_index].value_container->linked_value;
            each_key[track_index].value_container = each_key[track_index].value_container->next;
            return value;
        }
    }
}

char *reduce_get_next(char *key, int partition_number)
{
    bucket_chained_kv *each_key = reducer_table[partition_number].all_keys;
    while (1)
    {
        int track_index = reducer_table[partition_number].tracker;
        if (strcmp(key, each_key[track_index].unique_key) != 0)
        {
            reducer_table[partition_number].tracker++;
        }
        else
        {
            if (each_key[track_index].value_container == NULL)
                return NULL;

            string value = each_key[track_index].value_container->linked_value;
            each_key[track_index].value_container = each_key[track_index].value_container->next;
            return value;
        }
    }
}

void *mapper_runner(void *map_args)
{
    pthread_mutex_lock(&mapper_lock);
    int curr_file_index = getIndex(pthread_self());
    map_func(all_files[curr_file_index]);

    while ((curr_file_index + mapper_count) < total_files)
    {
        curr_file_index = curr_file_index + mapper_count;
        map_func(all_files[curr_file_index]);
    }

    int thread_index = getIndex(pthread_self());

    if (combine_func != NULL && mapper_table[thread_index].unique_key_count != 0)
    {

        int iter_index = 0;

        mapper_table[thread_index].all_keys = malloc(sizeof(bucket_chained_kv) * mapper_table[thread_index].unique_key_count);

        for (int i = 0; i < table_buckets; i++)
        {
            bucket_chained_kv *heap = mapper_table[thread_index].bucket_array[i].inner_heap;

            while (heap != NULL)
            {
                mapper_table[thread_index].all_keys[iter_index] = *heap;
                heap = heap->next;
                iter_index++;
            }
        }

        qsort(mapper_table[thread_index].all_keys, mapper_table[thread_index].unique_key_count, sizeof(bucket_chained_kv), comparator);

        for (int i = 0; i < mapper_table[thread_index].unique_key_count; i++)
        {
            string first_key = mapper_table[thread_index].all_keys[i].unique_key;
            combine_func(first_key, combine_get_next);
        }

        mapper_table[thread_index].tracker = 0;
        free(mapper_table[thread_index].all_keys);
    }

    pthread_mutex_unlock(&mapper_lock);
    return NULL;
}

void *reducer_runner(void *red_args)
{
    int partition_number = (*(struct reducer_args *)red_args).partition_number;
    int iter_index = 0;

    if (reducer_table[partition_number].unique_key_count == 0)
    {
        return NULL;
    }

    reducer_table[partition_number].all_keys = malloc(sizeof(bucket_chained_kv) * reducer_table[partition_number].unique_key_count);

    for (int i = 0; i < table_buckets; i++)
    {
        bucket_chained_kv *heap = reducer_table[partition_number].bucket_array[i].inner_heap;

        while (heap != NULL)
        {
            reducer_table[partition_number].all_keys[iter_index] = *heap;
            iter_index++;
            heap = heap->next;
        }
    }

    qsort(reducer_table[partition_number].all_keys, reducer_table[partition_number].unique_key_count, sizeof(bucket_chained_kv), comparator);

    for (int i = 0; i < reducer_table[partition_number].unique_key_count; i++)
    {
        string first_key = reducer_table[partition_number].all_keys[i].unique_key;
        reduce_func(first_key, NULL, reduce_get_next, partition_number);
    }

    free(red_args);
    free(reducer_table[partition_number].all_keys);

    return NULL;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Combiner combine,
            Partitioner partition)
{

    pthread_mutex_init(&mapper_lock, NULL);
    total_files = argc - 1;

    mapper_count = (num_mappers <= total_files) ? (num_mappers) : (total_files);
    reducer_count = num_reducers;

    all_files = malloc((argc - 1) * sizeof(char *));

    for (int i = 0; i < argc - 1; i++)
    {
        all_files[i] = argv[i + 1];
    }

    map_func = map;
    reduce_func = reduce;
    combine_func = combine;
    partition_func = partition;

    all_mappers = malloc(mapper_count * sizeof(pthread_t *));
    all_reducers = malloc(num_reducers * sizeof(pthread_t *));

    mapper_table = malloc(sizeof(kvtable) * mapper_count);
    reducer_table = malloc(sizeof(kvtable) * reducer_count);

    for (int i = 0; i < mapper_count; i++)
    {
        mapper_table[i].tracker = 0;

        mapper_table[i].bucket_array = malloc(sizeof(bucket) * table_buckets);

        for (int j = 0; j < table_buckets; j++)
        {
            mapper_table[i].bucket_array[j].inner_heap = NULL;
        }

        mapper_table[i].all_keys = NULL;

        mapper_table[i].unique_key_count = 0;
    }

    for (int i = 0; i < reducer_count; i++)
    {
        reducer_table[i].tracker = 0;

        reducer_table[i].bucket_array = malloc(sizeof(bucket) * table_buckets);

        for (int j = 0; j < table_buckets; j++)
        {
            reducer_table[i].bucket_array[j].inner_heap = NULL;
        }

        reducer_table[i].all_keys = NULL;

        reducer_table[i].unique_key_count = 0;
    }

    for (int i = 0; i < mapper_count; i++)
    {
        pthread_create(&all_mappers[i], NULL, mapper_runner, NULL);
    }

    for (int k = 0; k < mapper_count; k++)
    {
        pthread_join(all_mappers[k], NULL);
    }

    for (int i = 0; i < mapper_count; i++)
    {
        for (int j = 0; j < table_buckets; j++)
        {
            bucket_chained_kv *heap = mapper_table[i].bucket_array[j].inner_heap;

            while (heap != NULL)
            {
                free(heap->unique_key);
                chained_value *container = heap->value_container;

                while (container != NULL)
                {
                    free(container->linked_value);
                    chained_value *chain_item = container->next;
                    free(container);
                    container = chain_item;
                }

                bucket_chained_kv *inner_key = heap->next;
                free(heap);
                heap = inner_key;
            }
        }
        free(mapper_table[i].bucket_array);
    }

    free(mapper_table);

    struct reducer_args *red_args;

    for (int i = 0; i < reducer_count; i++)
    {
        red_args = malloc(sizeof(struct reducer_args));
        (*red_args).partition_number = i;

        pthread_create(&all_reducers[i], NULL, reducer_runner, red_args);
    }

    for (int m = 0; m < reducer_count; m++)
    {
        pthread_join(all_reducers[m], NULL);
    }

    for (int i = 0; i < reducer_count; i++)
    {
        for (int j = 0; j < table_buckets; j++)
        {
            bucket_chained_kv *heap = reducer_table[i].bucket_array[j].inner_heap;

            while (heap != NULL)
            {
                free(heap->unique_key);
                chained_value *container = heap->value_container;

                while (container != NULL)
                {
                    free(container->linked_value);
                    chained_value *chain_item = container->next;
                    free(container);
                    container = chain_item;
                }

                bucket_chained_kv *inner_key = heap->next;
                free(heap);
                heap = inner_key;
            }
        }
        free(reducer_table[i].bucket_array);
    }

    free(reducer_table);

    free(all_mappers);
    free(all_reducers);
    free(all_files);
}