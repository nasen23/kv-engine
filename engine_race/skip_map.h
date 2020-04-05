#ifndef __INDEX_H_
#define __INDEX_H_

#include "include/polar_string.h"
#include <pthread.h>
#include <random>
#include <vector>

namespace polar_race {

const static int KEY_MAX_LENGTH = 32;
const static int SKIPLIST_MAX_LEVEL = 18;
const static int INDEX_FILE_SIZE = 8 * 1024 * 1024;

struct Location {
  int32_t gen;
  uint32_t pos;
  uint32_t len;
};

const static Location INDEX_NOT_FOUND = {-1, 0, 0};

struct Item {
  char key[KEY_MAX_LENGTH] = {0};
  Location loc;
  uint32_t next[SKIPLIST_MAX_LEVEL];
  uint32_t level;
};

class SkipMap {
public:
  SkipMap(const std::string name);
  ~SkipMap();
  const Location &get(const PolarString &key) const;
  void insert(const PolarString &key, Location loc);

private:
  static const uint32_t FOOTER = 0;
  uint32_t rand_level();
  uint32_t alloc_node();
  Item *get_node(uint32_t num) const;
  void map_file();

  std::random_device rd;
  std::mt19937 generator;

  Item *nodes;
  Item *header;
  uint32_t *len;
  uint32_t *level;
  uint32_t cap;
  int fd;
  size_t fsize;
  void *fmap;
};

} // namespace polar_race

#endif // __INDEX_H_
