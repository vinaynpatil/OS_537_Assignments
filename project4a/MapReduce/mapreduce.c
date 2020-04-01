#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct node
{
    char *key;
    char *value;
    struct node *right_child; // right child
    struct node *left_child;  // left child
};

char *search(struct node *root, char *key)
{
    if (root == NULL || strcmp(key, root->key) == 0)
    {
        if (root == NULL)
        {
            return NULL;
        }
        else
        {
            return root->value;
        }
    }
    else if (strcmp(key, root->key) > 0)
        return search(root->right_child, key);
    else
        return search(root->left_child, key);
}

struct node *find_minimum(struct node *root)
{
    if (root == NULL)
        return NULL;
    else if (root->left_child != NULL)
        return find_minimum(root->left_child);
    return root;
}

void inorder(struct node *root)
{
    if (root != NULL) // checking if the root is not null
    {
        inorder(root->left_child);                                 // visiting left child
        printf(" Key, Value - %s, %s \n", root->key, root->value); // printing data at root
        inorder(root->right_child);                                // visiting right child
    }
}

//function to create a node
struct node *new_node(char *key, char *value)
{
    struct node *p;
    p = malloc(sizeof(struct node));
    p->key = key;
    p->value = value;
    p->left_child = NULL;
    p->right_child = NULL;

    return p;
}

struct node *insert(struct node **root, char *key, char *value)
{
    //searching for the place to insert
    if ((*root) == NULL)
        return new_node(key, value);
    else if (strcmp(key, (*root)->key) > 0) // x is greater. Should be inserted to right
        (*root)->right_child = insert(&(*root)->right_child, key, value);
    else // x is smaller should be inserted to left
        (*root)->left_child = insert(&(*root)->left_child, key, value);
    return (*root);
}

struct node *delete (struct node **root, char *key)
{
    if ((*root) == NULL)
        return NULL;
    if (strcmp(key, (*root)->key) > 0)
        (*root)->right_child = delete (&(*root)->right_child, key);
    else if (strcmp(key, (*root)->key) < 0)
        (*root)->left_child = delete (&(*root)->left_child, key);
    else
    {
        //No Children
        if ((*root)->left_child == NULL && (*root)->right_child == NULL)
        {
            free((*root));
            return NULL;
        }

        //One ChildgetNextMatchNode
        else if ((*root)->left_child == NULL || (*root)->right_child == NULL)
        {
            struct node *temp;
            if ((*root)->left_child == NULL)
                temp = (*root)->right_child;
            else
                temp = (*root)->left_child;
            free((*root));
            return temp;
        }

        //Two Children
        else
        {
            struct node *temp = find_minimum((*root)->right_child);
            (*root)->key = temp->key;
            (*root)->value = temp->value;
            (*root)->right_child = delete (&(*root)->right_child, temp->key);
        }
    }
    return (*root);
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

struct node **mapper_table;
struct node **reducer_table;

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
    char *new_key = strdup(key);
    char *new_val = strdup(value);

    int thread_index = getIndex(pthread_self());
    mapper_table[thread_index] = insert(&mapper_table[thread_index], new_key, new_val);
}

void MR_EmitToReducer(char *key, char *value)
{
    char *new_key = strdup(key);
    char *new_val = strdup(value);

    reducer_table[partitionGenerator(new_key, num_partitions)] = insert(&reducer_table[partitionGenerator(new_key, num_partitions)], new_key, new_val);
}

char *combine_get_next(char *key)
{
    int thread_index = getIndex(pthread_self());

    char *return_val = search(mapper_table[thread_index], key);

    if (return_val == NULL)
    {
        return NULL;
    }
    else
    {
        mapper_table[thread_index] = delete (&mapper_table[thread_index], key);
    }

    return return_val;
}

char *reduce_get_next(char *key, int partition_number)
{
    char *return_val = search(reducer_table[partition_number], key);

    if (return_val == NULL)
    {
        return NULL;
    }
    else
    {
        reducer_table[partition_number] = delete (&reducer_table[partition_number], key);
    }
    return return_val;
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
    mapper_table = malloc(sizeof(struct node *) * num_mappers);
    reducer_table = malloc(sizeof(struct node *) * num_reducers);

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