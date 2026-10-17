#include "mr.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct {
  mapper_t mapper;
  kvlist_t* input;
  kvlist_t* output;
} map_args_t;

typedef struct {
  reducer_t reducer;
  kvlist_t* input;
  kvlist_t* output;
} reduce_args_t;

void* map_thread(void* arg) {
  map_args_t* args = (map_args_t*)arg;

  kvlist_iterator_t* itor = kvlist_iterator_new(args->input);
  while (true) {
    kvpair_t* pair = kvlist_iterator_next(itor);
    if (pair == NULL) {
      break;
    }
    args->mapper(pair, args->output);
  }
  kvlist_iterator_free(&itor);

  return NULL;
}

void* reduce_thread(void* arg) {
  reduce_args_t* args = (reduce_args_t*)arg;

  kvlist_sort(args->input);

  kvlist_t* group = kvlist_new();
  char* current_key = NULL;

  kvlist_iterator_t* itor = kvlist_iterator_new(args->input);
  while (true) {
    kvpair_t* pair = kvlist_iterator_next(itor);
    if (pair == NULL) {
      break;
    }

    if (current_key != NULL && strcmp(current_key, pair->key) != 0) {
      args->reducer(current_key, group, args->output);
      kvlist_free(&group);
      group = kvlist_new();
    }

    current_key = pair->key;
    kvlist_append(group, kvpair_clone(pair));
  }
  kvlist_iterator_free(&itor);

  if (current_key != NULL) {
    args->reducer(current_key, group, args->output);
  }
  kvlist_free(&group);

  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  kvlist_t** split_lists = malloc(num_mapper * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_mapper; i++) {
    split_lists[i] = kvlist_new();
  }

  kvlist_iterator_t* itor = kvlist_iterator_new(input);
  size_t index = 0;
  while (true) {
    kvpair_t* pair = kvlist_iterator_next(itor);
    if (pair == NULL) {
      break;
    }
    kvlist_append(split_lists[index % num_mapper], kvpair_clone(pair));
    index++;
  }
  kvlist_iterator_free(&itor);

  pthread_t* map_threads = malloc(num_mapper * sizeof(pthread_t));
  map_args_t* map_args = malloc(num_mapper * sizeof(map_args_t));
  for (size_t i = 0; i < num_mapper; i++) {
    map_args[i].mapper = mapper;
    map_args[i].input = split_lists[i];
    map_args[i].output = kvlist_new();
    pthread_create(&map_threads[i], NULL, map_thread, &map_args[i]);
  }
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(map_threads[i], NULL);
  }

  kvlist_t** shuffle_lists = malloc(num_reducer * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_reducer; i++) {
    shuffle_lists[i] = kvlist_new();
  }

  for (size_t i = 0; i < num_mapper; i++) {
    itor = kvlist_iterator_new(map_args[i].output);
    while (true) {
      kvpair_t* pair = kvlist_iterator_next(itor);
      if (pair == NULL) {
        break;
      }
      size_t r = hash(pair->key) % num_reducer;
      kvlist_append(shuffle_lists[r], kvpair_clone(pair));
    }
    kvlist_iterator_free(&itor);
  }

  pthread_t* reduce_threads = malloc(num_reducer * sizeof(pthread_t));
  reduce_args_t* reduce_args = malloc(num_reducer * sizeof(reduce_args_t));
  for (size_t i = 0; i < num_reducer; i++) {
    reduce_args[i].reducer = reducer;
    reduce_args[i].input = shuffle_lists[i];
    reduce_args[i].output = kvlist_new();
    pthread_create(&reduce_threads[i], NULL, reduce_thread, &reduce_args[i]);
  }
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(reduce_threads[i], NULL);
  }

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_extend(output, reduce_args[i].output);
  }

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_free(&split_lists[i]);
    kvlist_free(&map_args[i].output);
  }
  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_free(&shuffle_lists[i]);
    kvlist_free(&reduce_args[i].output);
  }
  free(split_lists);
  free(shuffle_lists);
  free(map_threads);
  free(map_args);
  free(reduce_threads);
  free(reduce_args);
}
