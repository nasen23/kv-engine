#include <cassert>
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "db_slice.h"

namespace polar_race {

DbSlice::DbSlice(const std::string &dir, int id)
    : id(id), fprefix(dir + "." + std::to_string(id) + "."),
      index(fprefix + "index") {
  pthread_rwlock_init(&rwlock, nullptr);

  auto meta_name = fprefix + "meta";
  meta_fd = open(meta_name.c_str(), O_RDWR | O_CREAT, 0644);
  assert(meta_fd > 0);
  struct stat st = {};
  fstat(meta_fd, &st);

  bool new_db = false;
  if (st.st_size == 0) {
    ftruncate(meta_fd, sizeof(uint32_t) * 2);
    new_db = true;
  }

  meta_map = reinterpret_cast<uint32_t *>(
      mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, meta_fd, 0));

  if (new_db) {
    gen = 0;
    gen_fd[0] = open_new_datafile(0);
    map_gen_data(0);
  } else {
    gen = *meta_map;
    pos = *(meta_map + 1);
    for (uint32_t i = 0; i <= gen; i++) {
      auto name = fprefix + "data." + std::to_string(i);
      gen_fd[i] = open(name.c_str(), O_RDWR, 0644);
      assert(gen_fd[i] > 0);
      map_gen_data(i);
    }
  }
}

RetCode DbSlice::read(const PolarString &key, std::string *value) {
  pthread_rwlock_rdlock(&rwlock);
  auto res = index.get(key);
  if (__glibc_unlikely(res.gen == -1)) {
    pthread_rwlock_unlock(&rwlock);
    return kNotFound;
  }
  auto data_map = gen_map[res.gen];
  value->assign(data_map + res.pos, res.len);
  pthread_rwlock_unlock(&rwlock);
  return kSucc;
}

RetCode DbSlice::write(const PolarString &key, const PolarString &value) {
  pthread_rwlock_wrlock(&rwlock);
  auto len = (uint32_t)value.size();
  if (__glibc_unlikely(pos + len > FILE_SIZE)) {
    ++gen;
    gen_fd[gen] = open_new_datafile(gen);
    map_gen_data(gen);
  }

  memcpy(gen_map[gen] + pos, value.data(), len);
  index.insert(key, {(int32_t)gen, pos, len});
  pos += len;
  pthread_rwlock_unlock(&rwlock);
  return kSucc;
}

DbSlice::~DbSlice() {
  for (uint32_t i = 0; i <= gen; i++) {
    munmap(gen_map[i], FILE_SIZE);
    close(gen_fd[i]);
  }

  // write metadata to file
  *meta_map = gen;
  *(meta_map + 1) = pos;
  munmap(meta_map, 4096);
  close(meta_fd);

  pthread_rwlock_destroy(&rwlock);
}

int DbSlice::open_new_datafile(uint32_t gen) {
  auto name = fprefix + "data." + std::to_string(gen);
  int fd = open(name.c_str(), O_RDWR | O_CREAT, 0644);
  assert(fd > 0);
  ftruncate(fd, FILE_SIZE);
  pos = 0;
  return fd;
}

void DbSlice::map_gen_data(uint32_t gen) {
  void *data_map = mmap(nullptr, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                        gen_fd[gen], 0);
  madvise(data_map, FILE_SIZE, MADV_SEQUENTIAL);
  gen_map[gen] = reinterpret_cast<char *>(data_map);
}

} // namespace polar_race
