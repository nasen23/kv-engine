#ifndef __DB_SLICE_H_
#define __DB_SLICE_H_

#include "include/engine.h"
#include "skip_map.h"

#include <map>
#include <pthread.h>
#include <string>

namespace polar_race {

const static int MAX_GEN_NUMBER = 1 << 12;
const static int FILE_SIZE = 32 * 1024 * 1024;

class DbSlice {
public:
  DbSlice(const std::string &dir, int id);
  ~DbSlice();
  RetCode read(const PolarString &key, std::string *value);
  RetCode write(const PolarString &key, const PolarString &value);

private:
  pthread_rwlock_t rwlock;
  int id;
  std::string fprefix;
  SkipMap index;
  uint32_t gen;
  uint32_t pos;
  int meta_fd;
  uint32_t *meta_map;
  int gen_fd[MAX_GEN_NUMBER];
  char *gen_map[MAX_GEN_NUMBER];

  int open_new_datafile(uint32_t gen);
  void map_gen_data(uint32_t gen);
};

} // namespace polar_race

#endif // __DB_SLICE_H_
