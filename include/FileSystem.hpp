#ifndef FILESYSTEM_H
#define FILESYSTEM_H

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <cstring>
#include <sstream>

static bool ends_with(std::string const & value, std::string const & ending)
{
  if (ending.size() > value.size()) return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());

}

std::vector<std::string> ls(const std::string &path) {
  DIR *folder = opendir(path.c_str());
  struct dirent *next_file;
  std::vector<std::string> res;

  while ((next_file = readdir(folder)) != nullptr) {
    if (strcmp(next_file->d_name, ".") == 0 || strcmp(next_file->d_name, "..") == 0) {
      continue;
    }
    res.push_back(next_file->d_name);
  }

  closedir(folder);
  return res;
}

void delete_file(const std::string &path) {
  remove(path.c_str());
}

void delete_files(const std::string &path) {
  DIR *folder = opendir(path.c_str());
  if (folder == nullptr) {
    return;
  }

  struct dirent *next_file;
  std::stringstream ss;

  while ((next_file = readdir(folder)) != nullptr) {
    ss.str("");
    ss << path << "/" << next_file->d_name;
    remove(ss.str().c_str());
  }

  closedir(folder);
}

void delete_directory(const std::string &path) {
  delete_files(path);
  rmdir(path.c_str());
}

bool mkdir(const std::string &path) {
  return mkdir(path.c_str(), S_IRWXU | S_IRWXG) == 0;
}

std::string path_append(const std::string &p1, const std::string &p2) {
  if (ends_with(p1, "/")) {
    return p1 + p2;
  } else {
    return p1 + "/" + p2;
  }
}


#endif
