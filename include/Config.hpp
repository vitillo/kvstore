#ifndef CONFIG_H
#define CONFIG_H

#include <cstdint>
#include <cmath>
#include <sys/types.h>
#include <cassert>
#include <string>
#include <vector>

#include "FileSystem.hpp"

struct LevelConfig {
  LevelConfig() {}
  LevelConfig(const std::string &path,
              const std::string &db_name,
              uint32_t level,
              uint32_t table_size,
              uint32_t threshold,
              bool overwrite = false)
    : path(path),
      path_db(path_append(path, db_name)),
      path_level(path_append(path_db, std::to_string(level))),
      level(level),
      table_size(table_size),
      threshold(threshold),
      overwrite(overwrite){
  }

  std::string path;
  std::string path_db;
  std::string path_level;
  uint32_t level;
  uint32_t table_size;
  uint32_t threshold;
  bool overwrite;
};

std::vector<std::string> split(const std::string& s, const char& c) {
  std::string buff{""};
  std::vector<std::string> v;

  for(auto n : s) {
    if(n != c) {
      buff+=n;
    } else if(n == c && buff != "") {
      v.push_back(buff);
      buff = "";
    }
  }

  if(buff != "") {
    v.push_back(buff); 
  }

  return v;
}

struct Config{
  Config(const std::string &name,
         const std::string &path,
         int num_levels,
         uint32_t table_size,
         uint32_t threshold,
         uint32_t memtable_size,
         uint32_t parallelism = 1,
         bool overwrite = false):
      name(name),
      path(path),
      memtable_size(memtable_size),
      parallelism(parallelism) {
    assert(!name.empty());
    assert(!path.empty());

    auto directories = split(path, ',');
    assert(directories.size() == 1 || directories.size() == num_levels);
    if (directories.size() == 1) {
      for (int i = 1; i < num_levels; i++) {
        directories.push_back(directories[0]);
      }
    }

    for (int i = 0, t = threshold; i < num_levels; i++, t *= threshold) {
      levels.push_back(LevelConfig(directories[i], name, i, table_size, t, overwrite));
    }
  }

  static Config create_partition(const Config &config, uint partition) {
    auto new_name = config.name + "_" + std::to_string(partition);
    auto new_config = config;
    new_config.name = new_name;

    for (auto i = 0; i < new_config.levels.size(); i++){
      auto &level = new_config.levels[i];
      level.path_db = path_append(level.path, new_name);
      level.path_level = path_append(level.path_db, std::to_string(i));
    }

    return new_config;
  }

  std::vector<LevelConfig> levels;
  std::string name;
  std::string path;
  uint32_t memtable_size;
  uint32_t parallelism;
};

#endif
