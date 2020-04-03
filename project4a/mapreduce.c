#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

int mapper_count;
int reducer_count;
int NUM_FILES;
char **file_names;
int file_index;
const int table_buckets = 100000;

pthread_t *all_mappers;
pthread_t *all_reducers;

pthread_mutex_t locker;

Partitioner partition_func;
Mapper map_func;
Reducer reduce_func;
Combiner combine_func;

typedef struct __v_node
{
    char *value;
    struct __v_node *next;
} v_node;
typedef struct __k_node
{
    char *key;
    v_node *head;
    struct __k_node *next;
} k_node;
typedef struct __k_entry
{
    k_node *head;
} k_entry;

struct table
{
    k_entry map[100000];
    int key_num;
    k_node *sorted;
    int curr_visit;
};

struct table reducer_table[64];
struct table mapper_table[64];

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

char *get_combine_func(char *key)
{

    int thread_index = getIndex(pthread_self());
    k_node *arr = mapper_table[thread_index].sorted;
    char *value;
    while (1)
    {
        int curr = mapper_table[thread_index].curr_visit;
        if (strcmp(arr[curr].key, key) == 0)
        {
            if (arr[curr].head == NULL)
                return NULL;

            v_node *temp = arr[curr].head->next;
            value = arr[curr].head->value;
            arr[curr].head = temp;
            return value;
        }
        else
        {
            mapper_table[thread_index].curr_visit++;
            continue;
        }
        return NULL;
    }
}

char *get_func(char *key, int partition_number)
{
    k_node *arr = reducer_table[partition_number].sorted;
    char *value;
    while (1)
    {
        int curr = reducer_table[partition_number].curr_visit;
        if (strcmp(arr[curr].key, key) == 0)
        {
            if (arr[curr].head == NULL)
                return NULL;
            v_node *temp = arr[curr].head->next;
            value = arr[curr].head->value;
            arr[curr].head = temp;
            return value;
        }
        else
        {
            reducer_table[partition_number].curr_visit++;
            continue;
        }
        return NULL;
    }
}

int compareStr(const void *a, const void *b)
{
    char *n1 = ((k_node *)a)->key;
    char *n2 = ((k_node *)b)->key;
    int rc = strcmp(n1, n2);
    return rc;
}

void *mapper_wrapper(void *arg)
{
    pthread_mutex_lock(&locker);
    int curr_file_index = getIndex(pthread_self());
    map_func(file_names[curr_file_index]);

    while ((curr_file_index + mapper_count) < NUM_FILES)
    {
        curr_file_index = curr_file_index + mapper_count;
        map_func(file_names[curr_file_index]);
    }


    if (combine_func != NULL)
    {
        int thread_index = getIndex(pthread_self());

        if (mapper_table[thread_index].key_num == 0)
        {
            return NULL;
        }
        mapper_table[thread_index].sorted = malloc(sizeof(k_node) * mapper_table[thread_index].key_num);
        int count = 0;
        for (int i = 0; i < table_buckets; i++)
        {
            k_node *curr = mapper_table[thread_index].map[i].head;
            if (curr == NULL)
                continue;
            while (curr != NULL)
            {
                mapper_table[thread_index].sorted[count] = *curr;
                count++;
                curr = curr->next;
            }
        }

        qsort(mapper_table[thread_index].sorted, mapper_table[thread_index].key_num, sizeof(k_node), compareStr);

        for (int i = 0; i < mapper_table[thread_index].key_num; i++)
        {
            char *key = mapper_table[thread_index].sorted[i].key;
            combine_func(key, get_combine_func);
        }

        mapper_table[thread_index].curr_visit = 0;
        free(mapper_table[thread_index].sorted);
    }

    pthread_mutex_unlock(&locker);
    return NULL;
}

void *reducer_wrapper(void *arg1)
{
    int partition_number = *(int *)arg1;
    free(arg1);
    arg1 = NULL;
    if (reducer_table[partition_number].key_num == 0)
    {
        return NULL;
    }
    reducer_table[partition_number].sorted = malloc(sizeof(k_node) * reducer_table[partition_number].key_num);
    int count = 0;
    for (int i = 0; i < table_buckets; i++)
    {
        k_node *curr = reducer_table[partition_number].map[i].head;
        if (curr == NULL)
            continue;
        while (curr != NULL)
        {
            reducer_table[partition_number].sorted[count] = *curr;
            count++;
            curr = curr->next;
        }
    }

    qsort(reducer_table[partition_number].sorted, reducer_table[partition_number].key_num, sizeof(k_node), compareStr);

    for (int i = 0; i < reducer_table[partition_number].key_num; i++)
    {
        char *key = reducer_table[partition_number].sorted[i].key;
        reduce_func(key, NULL, get_func, partition_number);
    }

    free(reducer_table[partition_number].sorted);

    return NULL;
}

void MR_EmitToCombiner(char *key, char *value)
{
    int thread_index = getIndex(pthread_self());
    unsigned long map_number = MR_DefaultHashPartition(key, table_buckets);
    k_node *temp = mapper_table[thread_index].map[map_number].head;
    while (temp != NULL)
    {
        if (strcmp(temp->key, key) == 0)
        {
            break;
        }
        temp = temp->next;
    }
    v_node *new_v = malloc(sizeof(v_node));

    new_v->value = strdup(value);
    new_v->next = NULL;
    if (temp == NULL)
    {
        k_node *new_key = malloc(sizeof(k_node));

        new_key->head = new_v;
        new_key->next = mapper_table[thread_index].map[map_number].head;
        new_key->key = strdup(key);

        mapper_table[thread_index].map[map_number].head = new_key;
        mapper_table[thread_index].key_num++;
    }
    else
    {
        new_v->next = temp->head;
        temp->head = new_v;
    }
}

void MR_EmitToReducer(char *key, char *value)
{
    unsigned long partition_number = partition_func(key, reducer_count);
    unsigned long map_number = MR_DefaultHashPartition(key, table_buckets);
    k_node *temp = reducer_table[partition_number].map[map_number].head;
    while (temp != NULL)
    {
        if (strcmp(temp->key, key) == 0)
        {
            break;
        }
        temp = temp->next;
    }

    v_node *new_v = malloc(sizeof(v_node));

    new_v->value = strdup(value);
    new_v->next = NULL;
    if (temp == NULL)
    {
        k_node *new_key = malloc(sizeof(k_node));

        new_key->head = new_v;
        new_key->next = reducer_table[partition_number].map[map_number].head;
        new_key->key = strdup(key);

        reducer_table[partition_number].map[map_number].head = new_key;
        reducer_table[partition_number].key_num++;
    }
    else
    {
        new_v->next = temp->head;
        temp->head = new_v;
    }
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

    pthread_mutex_init(&locker, NULL);
    file_index = 0;
    NUM_FILES = argc - 1;

    mapper_count = (num_mappers <= NUM_FILES) ? (num_mappers) : (NUM_FILES);
    ;
    reducer_count = num_reducers;

    file_names = (argv + 1);
    map_func = map;
    reduce_func = reduce;
    combine_func = combine;
    partition_func = partition;

    for (int i = 0; i < mapper_count; i++)
    {
        mapper_table[i].key_num = 0;
        mapper_table[i].curr_visit = 0;
        mapper_table[i].sorted = NULL;
        for (int j = 0; j < table_buckets; j++)
        {
            mapper_table[i].map[j].head = NULL;
        }
    }

    for (int i = 0; i < num_reducers; i++)
    {
        reducer_table[i].key_num = 0;
        reducer_table[i].curr_visit = 0;
        reducer_table[i].sorted = NULL;
        for (int j = 0; j < table_buckets; j++)
        {
            reducer_table[i].map[j].head = NULL;
        }
    }

    all_mappers = malloc(mapper_count * sizeof(pthread_t *));
    all_reducers = malloc(num_reducers * sizeof(pthread_t *));

    for (int i = 0; i < mapper_count; i++)
    {
        pthread_create(&all_mappers[i], NULL, mapper_wrapper, NULL);
    }

    for (int k = 0; k < mapper_count; k++)
    {
        pthread_join(all_mappers[k], NULL);
    }

    for (int k = 0; k < mapper_count; k++)
    {
        for (int i = 0; i < table_buckets; i++)
        {
            k_node *curr = mapper_table[k].map[i].head;
            if (curr == NULL)
                continue;
            while (curr != NULL)
            {
                free(curr->key);
                curr->key = NULL;
                v_node *vcurr = curr->head;
                while (vcurr != NULL)
                {
                    free(vcurr->value);
                    vcurr->value = NULL;
                    v_node *temp = vcurr->next;
                    free(vcurr);
                    vcurr = temp;
                }
                vcurr = NULL;
                k_node *tempK = curr->next;
                free(curr);
                curr = tempK;
            }
            curr = NULL;
        }
    }

    for (int j = 0; j < num_reducers; j++)
    {
        void *arg = malloc(4);
        *(int *)arg = j;
        pthread_create(&all_reducers[j], NULL, reducer_wrapper, arg);
    }

    for (int m = 0; m < num_reducers; m++)
    {
        pthread_join(all_reducers[m], NULL);
    }

    for (int k = 0; k < num_reducers; k++)
    {
        for (int i = 0; i < table_buckets; i++)
        {
            k_node *curr = reducer_table[k].map[i].head;
            if (curr == NULL)
                continue;
            while (curr != NULL)
            {
                free(curr->key);
                curr->key = NULL;
                v_node *vcurr = curr->head;
                while (vcurr != NULL)
                {
                    free(vcurr->value);
                    vcurr->value = NULL;
                    v_node *temp = vcurr->next;
                    free(vcurr);
                    vcurr = temp;
                }
                vcurr = NULL;
                k_node *tempK = curr->next;
                free(curr);
                curr = tempK;
            }
            curr = NULL;
        }
    }

    free(all_mappers);
    free(all_reducers);
}
