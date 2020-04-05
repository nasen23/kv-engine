#include <cassert>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "skip_map.h"

namespace polar_race {

inline int fstrcmp(const char *a, const char *b) {
  // since I know key.len >= 8 bytes
  auto l = reinterpret_cast<const int64_t *>(a);
  auto r = reinterpret_cast<const int64_t *>(b);
  auto res = *l - *r;
  if (res < 0) {
    return -1;
  } else if (res > 0) {
    return 1;
  }
  return strcmp(a, b);
}

SkipMap::SkipMap(const std::string name) : generator(rd()) {
  struct stat st = {};
  fd = open(name.c_str(), O_RDWR | O_CREAT, 0644);
  assert(fd > 0);
  fstat(fd, &st);
  fsize = (size_t)st.st_size;

  bool init = false;
  if (fsize == 0) {
    ftruncate(fd, INDEX_FILE_SIZE);
    fsize = INDEX_FILE_SIZE;
    init = true;
  }

  size_t map_size = ((fsize + 4096 - 1) / 4096) * 4096;
  cap =
      (uint32_t)(map_size - 2 * sizeof(uint32_t) - sizeof(Item)) / sizeof(Item);

  fmap = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  assert(fmap != MAP_FAILED);
  map_file();

  if (init) {
    *len = 0;
    *level = 0;
    *header = {};
  }
}

SkipMap::~SkipMap() {
  munmap(fmap, fsize);
  close(fd);
}

const Location &SkipMap::get(const PolarString &key) const {
  if (__glibc_unlikely(*len == 0)) {
    return INDEX_NOT_FOUND;
  }
  auto node = header;
  for (int i = *level; i >= 0; i--) {
    while (true) {
      if (node->next[i] == FOOTER) {
        break;
      }
      auto next = get_node(node->next[i]);
      int res = fstrcmp(key.data(), next->key);
      if (res == 0) {
        node = next;
        goto found;
      } else if (res > 0) {
        break;
      } else {
        node = next;
      }
    }
  }

  node = get_node(node->next[0]);
  if (__glibc_likely(fstrcmp(key.data(), node->key) == 0)) {
  found:
    return node->loc;
  } else {
    return INDEX_NOT_FOUND;
  }
}

void SkipMap::insert(const PolarString &key, Location loc) {
  Item *update[SKIPLIST_MAX_LEVEL];

  auto node = header;
  for (int i = *level; i >= 0; i--) {
    while (true) {
      if (node->next[i] == FOOTER) {
        break;
      }
      auto next = get_node(node->next[i]);
      // printf("len: %d, node next: %d\n", *len, node->next[i]);
      int res = fstrcmp(key.data(), next->key);
      // printf("node key: %s, search key: %s\n", node->key, key.data());
      if (res == 0) {
        node = next;
        goto insert_found;
      } else if (res > 0) {
        break;
      } else {
        node = next;
      }
    }
    update[i] = node;
  }

  if (node->next[0] != FOOTER) {
    node = get_node(node->next[0]);
    if (__glibc_likely(fstrcmp(node->key, key.data()) == 0)) {
    insert_found:
      node->loc = loc;
      return;
    }
  }

  uint32_t new_level = rand_level();

  if (new_level > *level) {
    new_level = ++*level;
    update[new_level] = header;
  }

  uint32_t idx = alloc_node();
  auto new_node = get_node(idx);
  memcpy(new_node->key, key.data(), key.size());
  new_node->key[key.size()] = '\0';
  new_node->loc = loc;

  for (int i = new_level; i >= 0; i--) {
    node = update[i];
    new_node->next[i] = node->next[i];
    node->next[i] = idx;
  }
}

inline uint32_t SkipMap::rand_level() {
  std::uniform_int_distribution<int> gen_level(1, SKIPLIST_MAX_LEVEL - 1);
  uint32_t level = gen_level(generator);
  return level;
}

inline uint32_t SkipMap::alloc_node() {
  if (__glibc_unlikely(*len + 1 >= cap)) {
    ftruncate(fd, fsize * 2);
    fmap = mremap(fmap, fsize, fsize * 2, MREMAP_MAYMOVE);
    assert(fmap != MAP_FAILED);
    fsize *= 2;
    cap =
        (uint32_t)(fsize - 2 * sizeof(uint32_t) - sizeof(Item)) / sizeof(Item);
    map_file();
  }
  ++*len;
  return *len;
}

inline Item *SkipMap::get_node(uint32_t num) const { return nodes + num - 1; }

void SkipMap::map_file() {
  madvise(fmap, fsize, MADV_RANDOM);
  len = reinterpret_cast<uint32_t *>(fmap);
  level = reinterpret_cast<uint32_t *>(len + 1);
  header = reinterpret_cast<Item *>(level + 1);
  nodes = reinterpret_cast<Item *>(header + 1);
}

} // namespace polar_race
