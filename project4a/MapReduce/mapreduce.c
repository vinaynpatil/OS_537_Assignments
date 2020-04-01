#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct kvpair
{
    char *key;
    char *value;
    struct kvpair *next;
};

struct kvpair *createNewNode(char *key, char *value)
{
    struct kvpair *node = (struct kvpair *)malloc(sizeof(struct kvpair));
    node->key = key;
    node->value = value;
    node->next = NULL;
    return node;
}

void insertNode(struct kvpair **head, struct kvpair *node)
{
    struct kvpair *current;

    if (*head == NULL || strcmp((*head)->key, node->key) >= 0)
    {
        node->next = *head;
        *head = node;
    }
    else
    {
        current = *head;
        while (current->next != NULL && strcmp(current->next->key, node->key) < 0)
        {
            current = current->next;
        }
        node->next = current->next;
        current->next = node;
    }
}

void printList(struct kvpair *head)
{
    struct kvpair *temp = head;
    while (temp != NULL)
    {
        printf("Key - %s  \n", temp->key);
        printf("Value - %s  \n", temp->value);
        temp = temp->next;
    }
}

char *getNextMatchNode(struct kvpair **head, char *key)
{
    if (*head == NULL || strcmp((*head)->key, key) != 0)
    {
        return NULL;
    }
    else
    {
        struct kvpair *current = *head;
        *head = current->next;
        return current->value;
    }
}

struct mapper_args
{
    Mapper map;
    Combiner combine;
    int num_maps;
    char *file_name;
    int index;
};

struct reducer_args
{
    Reducer reduce;
    int partition_number;
};

struct kvpair **mapper_table;
struct kvpair **reducer_table;

pthread_mutex_t lock;
pthread_t *all_mappers;
pthread_t *all_reducers;
int mapper_count;
int num_files;
int num_map_threads;
char **all_files;

Partitioner partitionGenerator;
int num_partitions;

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

void MR_EmitToCombiner(char *key, char *value)
{
    int thread_index = getIndex(pthread_self());
    struct kvpair *node = createNewNode(key, value);
    insertNode(&mapper_table[thread_index], node);
}

void MR_EmitToReducer(char *key, char *value)
{
    struct kvpair *node = createNewNode(key, value);
    insertNode(&reducer_table[partitionGenerator(key, num_partitions)], node);
}

char *combine_get_next(char *key)
{
    int thread_index = getIndex(pthread_self());
    return getNextMatchNode(&mapper_table[thread_index], key);
}

char *reduce_get_next(char *key, int partition_number)
{
    return getNextMatchNode(&reducer_table[partition_number], key);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    
    return hash % num_partitions;
}

void *mapper_runner(void *map_args)
{
    pthread_mutex_lock(&lock);
    struct mapper_args *temp_args = (struct mapper_args *)map_args;
    int curr_file_index = (*temp_args).index;
    (*temp_args).map((*temp_args).file_name);

    while ((curr_file_index + num_map_threads) < num_files)
    {
        curr_file_index = curr_file_index + num_map_threads;
        (*temp_args).map(all_files[curr_file_index]);
    }

    if ((*temp_args).combine != NULL)
    {
        while (mapper_table[(*temp_args).index] != NULL)
        {
            (*temp_args).combine(mapper_table[(*temp_args).index]->key, combine_get_next);
        }
    }

    pthread_mutex_unlock(&lock);

    return NULL;
}

void *reducer_runner(void *red_args)
{
    pthread_mutex_lock(&lock);
    struct reducer_args *temp_args = (struct reducer_args *)red_args;
    while (reducer_table[(*temp_args).partition_number] != NULL)
    {
        (*temp_args).reduce(reducer_table[(*temp_args).partition_number]->key, NULL, reduce_get_next, (*temp_args).partition_number);
    }
    pthread_mutex_unlock(&lock);
    return NULL;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Combiner combine,
            Partitioner partition)
{
    mapper_table = malloc(sizeof(struct kvpair *) * num_mappers);
    reducer_table = malloc(sizeof(struct kvpair *) * num_reducers);

    for (int i = 0; i < num_mappers; i++)
    {
        mapper_table[i] = NULL;
    }

    for (int i = 0; i < num_reducers; i++)
    {
        reducer_table[i] = NULL;
    }

    all_mappers = malloc(num_mappers * sizeof(pthread_t *));
    mapper_count = num_mappers;

    num_files = argc - 1;

    struct mapper_args *map_args;

    num_map_threads = (num_mappers <= num_files) ? (num_mappers) : (num_files);

    all_files = malloc((argc - 1) * sizeof(char *));

    partitionGenerator = partition;
    num_partitions = num_reducers;

    for (int i = 0; i < argc - 1; i++)
    {
        all_files[i] = argv[i + 1];
    }

    for (int i = 0; i < num_map_threads; i++)
    {
        map_args = malloc(sizeof(struct mapper_args));

        (*map_args).combine = combine;
        (*map_args).map = map;
        (*map_args).file_name = argv[i + 1];
        (*map_args).num_maps = num_mappers;
        (*map_args).index = i;

        pthread_create(&all_mappers[i], NULL, mapper_runner, (void *)map_args);
    }

    for (int i = 0; i < num_map_threads; i++)
    {
        pthread_join(all_mappers[i], NULL);
    }

    all_reducers = malloc(num_reducers * sizeof(pthread_t *));

    struct reducer_args *red_args;

    for (int i = 0; i < num_reducers; i++)
    {
        red_args = malloc(sizeof(struct reducer_args));

        (*red_args).reduce = reduce;
        (*red_args).partition_number = i;

        pthread_create(&all_reducers[i], NULL, reducer_runner, (void *)red_args);
    }

    for (int i = 0; i < num_reducers; i++)
    {
        pthread_join(all_reducers[i], NULL);
    }
}