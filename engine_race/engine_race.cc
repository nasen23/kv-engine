// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

namespace polar_race {

RetCode Engine::Open(const std::string &name, Engine **eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
  *eptr = new EngineRace(name);
  return kSucc;
}

EngineRace::EngineRace(const std::string &dir) {
  for (int i = 0; i < DATABASE_SLICE_COUNT; i++) {
    db_slices[i] = new DbSlice(dir, i);
  }
}

// 2. Close engine
EngineRace::~EngineRace() {
  for (int i = 0; i < DATABASE_SLICE_COUNT; i++) {
    delete db_slices[i];
  }
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
  uint8_t id = (uint8_t)key.data()[0];
  return db_slices[id]->write(key, value);
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString &key, std::string *value) {
  uint8_t id = (uint8_t)key.data()[0];
  return db_slices[id]->read(key, value);
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                          Visitor &visitor) {
  return kNotSupported;
}

} // namespace polar_race
