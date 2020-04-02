#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "mapreduce.h"
#include <assert.h>
#include <pthread.h>
#include <sys/types.h>

// ====== Type for the data structure ========
// for keeping sorted keys for each partition
typedef struct sort_t
{
    char *key;
} sort;

// Value structure
typedef struct node_value_t
{
    char *value;
    struct node_value_t *next;
} node_value;

// linked list for seperate chaining
typedef struct node_t
{
    char *key;
    node_value *value;
    struct node_t *next;
    void *next_value;
} node;

//hash table structure
typedef struct hash_table_t
{
    int size;     //size of table
    node **table; // Fixed size hash table
    pthread_mutex_t **lock;
    sort **uniq_keys;
    int num_keys;
    void *next_value;
    pthread_mutex_t *memory_lock;
} hash_table;

// Struct for sending parameters to mapper threads
typedef struct proc_files_params_t
{
    int numFiles;
    Mapper map;
    Combiner combine;
    char **files;
} proc_files;

// Struct for sending parameters to reducer threads
typedef struct proc_data_structure_params_t
{
    int parts_num;
    Reducer reduce;
} proc_ds;

// ======= Global variables ==========
hash_table **inters;   // for holding the inter
hash_table **parts;   // for holding the partitions
Partitioner partFunc; // Global variable for partition function
int numReducers;
int numMappers;
int filesProcessed;
pthread_t *mappers_id;
pthread_mutex_t global_file_lock = PTHREAD_MUTEX_INITIALIZER;
//pthread_mutex_t force_lock = PTHREAD_MUTEX_INITIALIZER;

// ====== Wrappers for pthread library ==========
#define hash_table_size 100003 //102161
#define Pthread_mutex_lock(m) assert(pthread_mutex_lock(m) == 0);
#define Pthread_mutex_unlock(m) assert(pthread_mutex_unlock(m) == 0);
#define Pthread_create(thread, attr, start_routine, arg) assert(pthread_create(thread, attr, start_routine, arg) == 0);
#define Pthread_join(thread, value_ptr) assert(pthread_join(thread, value_ptr) == 0);



int getIndex(pthread_t thread_id)
{
    for (int i = 0; i < numMappers; i++)
    {
        if (pthread_equal(thread_id, mappers_id[i]) != 0)
        {
            return i;
        }
    }
    return -1;
}

// ================ Sorting functions ===========

int comp_sort(const void *elem1, const void *elem2)
{
    sort *key1 = *(sort **)elem1;
    sort *key2 = *(sort **)elem2;
    return strcmp(key1->key, key2->key);
}

//======== Data structure implementation =========

// Intialize the hash table
hash_table *create_hash_table(int size)
{

    // New hash table
    hash_table *new_table;

    if (size < 1)
        return NULL;

    // allocating mem for the table structure
    new_table = malloc(sizeof(hash_table));
    if (new_table == NULL)
    {
        return NULL;
    }

    // Allocating memory for table
    new_table->table = malloc(sizeof(node *) * size);
    if (new_table->table == NULL)
    {
        return NULL;
    }

    // Allocating memory for the lock array
    new_table->lock = malloc(sizeof(pthread_mutex_t *) * size);
    if (new_table->lock == NULL)
    {
        return NULL;
    }

    // Allocate memory for memory_lock
    new_table->memory_lock = malloc(sizeof(pthread_mutex_t)); // Allocating memory for the lock
    pthread_mutex_init(new_table->memory_lock, NULL);         // Initializing the lock

    // Setting sort to NULL
    new_table->uniq_keys = NULL;

    // setting num_keys to zero
    new_table->num_keys = 0;

    // set the next_value of the hast_table to NULL
    new_table->next_value = NULL;

    /*initialize the lements of the table*/
    for (int i = 0; i < size; i++)
    {
        new_table->table[i] = NULL;
        new_table->lock[i] = malloc(sizeof(pthread_mutex_t)); // Allocating memory for the lock
        pthread_mutex_init(new_table->lock[i], NULL);         // Initializing the lock
    }

    /*set the table's size*/
    new_table->size = size;
    return new_table;
}

// the hash function - Assuming to be correct
int hash(hash_table *hashtable, char *str)
{
    unsigned int hashval;

    //hashing from 0
    hashval = 0;

    //for each char, we multiply old hash by 31 and add curr char
    for (; *str != '\0'; str++)
    {
        hashval = *str + (hashval << 5) - hashval;
    }

    //we return hash value mod hashtable size so it fits in to necessary range
    return hashval % hashtable->size;
}

/* Lookups the given key and return key of node type 
 * otherwise returns NULL
 */
node *lookup(hash_table *hashtable, char *str)
{

    node *list;
    unsigned int hashval = hash(hashtable, str);

    // Linearly search through the bucket incase there is a collision for the
    // current hashtable
    for (list = hashtable->table[hashval]; list != NULL; list = list->next)
    {
        if (strcmp(str, list->key) == 0)
            return list;
    }

    return NULL;
}

/* Inserts the given key in the given hashtable by creating
 * new objects of node and node_value type
 */
int insert(hash_table *hashtable, char *str, char *val)
{

    node *new_list;
    node_value *new_value;

    // Allocate value for the value node
    if ((new_value = malloc(sizeof(node_value))) == NULL)
    {
        return 1; // error value
    }

    unsigned int hashval = hash(hashtable, str);

    // grabbing the lock for current bucket
    pthread_mutex_lock(hashtable->lock[hashval]);

    // Look for the key in hashtable. This will return the list of values
    node *key = lookup(hashtable, str);

    if (key == NULL)
    {
        // Allocate memory for new node for the given key
        if ((new_list = malloc(sizeof(node))) == NULL)
        {
            return 1; // error value
        }

        // insert into list
        // also need to insert the value here
        new_list->key = strdup(str);
        new_list->next = hashtable->table[hashval];
        hashtable->table[hashval] = new_list;

        new_value->value = strdup(val);
        new_value->next = NULL;
        new_list->value = new_value;

        // Updating next_value for get_next
        new_list->next_value = (void *)new_value;

        // Incrementing the count of keys in a hashtable
        pthread_mutex_lock(hashtable->memory_lock);
        hashtable->num_keys++;

        // Allocating memory for keys
        hashtable->uniq_keys = realloc(hashtable->uniq_keys, sizeof(sort *) * hashtable->num_keys);

        // Adding key to the sort array
        hashtable->uniq_keys[hashtable->num_keys - 1] = malloc(sizeof(sort));
        hashtable->uniq_keys[hashtable->num_keys - 1]->key = strdup(str);
        pthread_mutex_unlock(hashtable->memory_lock);
    }
    else
    {
        new_value->value = strdup(val);
        new_value->next = key->value;
        key->value = new_value;
        key->next_value = (void *)new_value; // Updating the next value for get_next
    }

    // Leaving the lock for current bucket
    pthread_mutex_unlock(hashtable->lock[hashval]);

    return 0;
}

void free_array(sort **words, int size)
{
    for (int i = 0; i < size; i++)
    {
        free(words[i]->key);
        free(words[i]);
    }
    free(words);
}

void free_values(node_value *nv)
{
    node_value *temp = nv;
    for (; temp != NULL; temp = nv)
    {
        nv = nv->next;
        free(temp->value); // Freeing the strdup memory
        free(temp);        // Freeing the node of the linkedlist
    }
}

void free_node(node *n)
{
    // Going over all the values for the given bucket
    node *temp = n;
    for (; temp != NULL; temp = n)
    {
        n = n->next;
        free_values(temp->value);
        free(temp->key); // Freeing the strdup memory
        free(temp);      // Freeing hte node of the linkedlist
    }
}

void free_hash_table(hash_table *ht)
{
    for (int i = 0; i < ht->num_keys; i++)
    {
        free(ht->uniq_keys[i]->key);
        free(ht->uniq_keys[i]);
    }
    free(ht->uniq_keys);

    // Going over all the elements in the hashtable
    for (int i = 0; i < ht->size; i++)
    {
        free(ht->lock[i]); // Free the lock for that bucket
        if (ht->table[i] != NULL)
        {
            node *temp = ht->table[i];

            free_node(temp);
        }
    }
    free(ht->memory_lock);
    free(ht->lock);
    free(ht->table); // Freeing the memory for the hashtable - array
    free(ht);        // Freeing the hashtable pointer
}

void free_partition(hash_table **p)
{
    // Freeing the memory allocated for hash tables
    for (int i = 0; i < numReducers; i++)
    {
        free_hash_table(p[i]);
    }
    free(p);
}

void dump_hash_table(hash_table *ht)
{
    // Going over the buckets
    for (int i = 0; i < ht->size; i++)
    {
        printf("    ");

        // Checking if the bucket is not null
        if (ht->table[i] != NULL)
        {
            printf("%d: \n", i);
            node *temp = ht->table[i];

            // Printing out the keys in that bucket
            for (; temp != NULL; temp = temp->next)
            {
                printf("        ");
                printf("\"%s\" -> [", temp->key);
                node_value *temp1 = temp->value;

                // Printing out the values in for that key
                for (; temp1 != NULL; temp1 = temp1->next)
                {
                    printf(" \"%s\" ->", temp1->value);
                }
                printf(" NULL ]\n");
            }
        }
        else
        {
            printf("%d:", i);
        }

        printf("\n");
    }
}

void dump_hash_table_keys(hash_table *ht)
{
    // Going over the buckets
    for (int i = 0; i < ht->size; i++)
    {

        // Checking if the bucket is not null
        if (ht->table[i] != NULL)
        {
            node *temp = ht->table[i];

            // Printing out the keys in that bucket
            for (; temp != NULL; temp = temp->next)
            {
                printf("%s\n", temp->key);
                //node_value* temp1 = temp->value;
            }
        }
    }
}

void dump_partitions(hash_table **p)
{
    for (int i = 0; i < numReducers; i++)
    {
        printf("Parition: %d -->\n", i);
        dump_hash_table(p[i]);
        //dump_hash_table_keys(p[i]);
    }
}

//======== Function to implement =================

void MR_EmitToCombiner(char *key, char *value)
{
    int thread_index = getIndex(pthread_self());
    //Pthread_mutex_lock(&force_lock);
    insert(inters[thread_index], key, value);
    //Pthread_mutex_unlock(&force_lock);
}

void MR_EmitToReducer(char *key, char *value)
{
    unsigned long partNum = (*partFunc)(key, numReducers);
    //Pthread_mutex_lock(&force_lock);
    insert(parts[partNum], key, value);
    //Pthread_mutex_unlock(&force_lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


char *get_next_comb(char *key)
{
    int thread_index = getIndex(pthread_self());
    hash_table *hashtable = inters[thread_index];

    node *n = NULL;
    if (hashtable->next_value == NULL)
    {
        n = lookup(inters[thread_index], key);
        //printf("hashtable next value %p \n",hashtable->next_value);
        hashtable->next_value = n->next_value;
        n->next_value = NULL; //(void*)temp->next;
    }

    char *value_to_ret = NULL;
    if (hashtable->next_value != NULL)
    {
        node_value *temp = ((node_value *)(hashtable->next_value));
        value_to_ret = temp->value;
        hashtable->next_value = temp->next;
    }
    else
    {
        return value_to_ret;
    }

    return value_to_ret;
}

/* This will keep the thread busy as long as there 
 * are more files to process
 */
void process_files(proc_files *params)
{

    int numFiles = params->numFiles;
    char **files = params->files;
    Mapper map = params->map;
    Combiner combine = params->combine;

    while (1)
    {

        int file_to_proc;

        // Find the file to process
        Pthread_mutex_lock(&global_file_lock);
        if (filesProcessed == numFiles)
        {
            Pthread_mutex_unlock(&global_file_lock); // Drop the lock before exiting this function
            return;
        }
        file_to_proc = filesProcessed;
        filesProcessed++;

        Pthread_mutex_unlock(&global_file_lock);

        // Process that file
        (*map)(files[file_to_proc]); // Calling the function from the thread

        if (combine != NULL)
        {
            int thread_index = getIndex(pthread_self());
            qsort(inters[thread_index]->uniq_keys, inters[thread_index]->num_keys, sizeof(sort), comp_sort);

            for (int i = 0; i < inters[thread_index]->num_keys; i++)
            {
                (*combine)(inters[thread_index]->uniq_keys[i]->key, get_next_comb);
            }
        }
    }
}

char *get_next(char *key, int partition_num)
{
    hash_table *hashtable = parts[partition_num];

    node *n = NULL;
    if (hashtable->next_value == NULL)
    {
        n = lookup(parts[partition_num], key);
        //printf("hashtable next value %p \n",hashtable->next_value);
        hashtable->next_value = n->next_value;
        n->next_value = NULL; //(void*)temp->next;
    }

    char *value_to_ret = NULL;
    if (hashtable->next_value != NULL)
    {
        node_value *temp = ((node_value *)(hashtable->next_value));
        value_to_ret = temp->value;
        hashtable->next_value = temp->next;
    }
    else
    {
        return value_to_ret;
    }

    return value_to_ret;
}

void process_data_struct(proc_ds *params)
{

    Reducer reduce = params->reduce;
    qsort(parts[params->parts_num]->uniq_keys, parts[params->parts_num]->num_keys, sizeof(sort), comp_sort);

    for (int i = 0; i < parts[params->parts_num]->num_keys; i++)
    {
        (*reduce)(parts[params->parts_num]->uniq_keys[i]->key, NULL, get_next, params->parts_num);
    }
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Combiner combine, Partitioner partition)
{
    // Setting the global variables
    partFunc = partition;
    numReducers = num_reducers;
    numMappers = num_mappers;

    // parts is a global variable for the partitions
    inters = malloc(sizeof(hash_table *) * num_mappers);
    parts = malloc(sizeof(hash_table *) * numReducers);

    if (inters==0 || parts == 0)
    {
        printf("cannot allocate memory\n");
        return;
    }

    for (int i = 0; i < numMappers; i++)
    {
        inters[i] = create_hash_table(hash_table_size);
    }

    // Creating one hash table for each partition
    for (int i = 0; i < numReducers; i++)
    {
        parts[i] = create_hash_table(hash_table_size);
    }

    // Initialize files processed
    filesProcessed = 1; // Set it to 1 when using arc and argv because the first value is the program name

    // # of mappers to create
    int numthreads = num_mappers;

    // Getting this from argc
    int numFiles = argc;

    // Setting the structure to send parameters to process_files
    proc_files params;
    params.numFiles = numFiles;
    params.files = argv; // setting the input files
    params.map = map;
    params.combine = combine;

    // Variable for mappers
    mappers_id = malloc(num_mappers * sizeof(pthread_t *));

    // Function to call from each thread
    void (*pf)(proc_files *) = &process_files;

    // Calling process_files from threads
    for (int i = 0; i < numthreads; i++)
    {
        Pthread_create(&mappers_id[i], NULL, (void *)pf, (void *)&params);
    }

    // Join all the mappers
    for (int i = 0; i < numthreads; i++)
    {
        Pthread_join(mappers_id[i], NULL);
    }

    // DEBUG:
    //dump_partitions(parts);

    pthread_t reducers_id[numReducers];
    proc_ds params_reducers[numReducers];

    for (int i = 0; i < numReducers; i++)
    {
        params_reducers[i].parts_num = i;
        params_reducers[i].reduce = reduce;
    }

    void (*pds)(proc_ds *) = &process_data_struct;

    for (int i = 0; i < numReducers; i++)
    {
        Pthread_create(&reducers_id[i], NULL, (void *)pds, (void *)&params_reducers[i]);
    }

    // Join all the reducers
    for (int i = 0; i < numReducers; i++)
    {
        Pthread_join(reducers_id[i], NULL);
    }

    free_partition(parts);
    free_partition(inters);
}