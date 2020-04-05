// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include "include/engine.h"
#include <string>

#include "db_slice.h"

namespace polar_race {

// this is same as concurrency
const static int DATABASE_SLICE_COUNT = 256;

class EngineRace : public Engine {
public:
  static RetCode Open(const std::string &name, Engine **eptr);

  explicit EngineRace(const std::string &dir);

  ~EngineRace();

  RetCode Write(const PolarString &key, const PolarString &value) override;

  RetCode Read(const PolarString &key, std::string *value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString &lower, const PolarString &upper,
                Visitor &visitor) override;

private:
  DbSlice *db_slices[DATABASE_SLICE_COUNT];
};

} // namespace polar_race

#endif // ENGINE_RACE_ENGINE_RACE_H_
