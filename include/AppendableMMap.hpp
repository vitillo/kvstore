#ifndef APPENDABLE_MMAP
#define APPENDABLE_MMAP

#include <cassert>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

#include "FileSystem.hpp"

class AppendableMMap{
public:
  AppendableMMap(const std::string &filename): m_filename(filename) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
      throw std::system_error(errno, std::system_category());
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
      throw std::system_error(errno, std::system_category());
    }

    m_size = sb.st_size;
    m_tail_index = m_size - 1;
    m_buffer = reinterpret_cast<char *>(mmap(0, m_size, PROT_READ, MAP_SHARED, fd, 0));
    if (m_buffer == MAP_FAILED) {
      throw std::system_error(errno, std::system_category());
    }

    if (close(fd) == -1) {
      throw std::system_error(errno, std::system_category());
    }
  }

  AppendableMMap(uint32_t size, const std::string &filename): m_filename(filename), m_size(size) {
    int fd = open(filename.c_str(), O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd == -1) {
      throw std::system_error(errno, std::system_category());
    }

    if (ftruncate(fd, size) == -1) {
      throw std::system_error(errno, std::system_category());
    }

    m_tail_index = size - 1;
    m_buffer = reinterpret_cast<char *>(mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    if (m_buffer == MAP_FAILED) {
      throw std::system_error(errno, std::system_category());
    }

    if (close(fd) == -1) {
      throw std::system_error(errno, std::system_category());
    }
  }

  AppendableMMap(uint32_t size): m_size(size) {
    m_buffer = reinterpret_cast<char *>(mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    m_tail_index = size - 1;
  }

  ~AppendableMMap() {
    assert(free() >= 0);

    if (msync(m_buffer, m_size, MS_SYNC) == -1) {
      throw std::system_error(errno, std::system_category());
    }

    if (munmap(const_cast<void *>(reinterpret_cast<const void *>(m_buffer)), m_size) == -1) {
      throw std::system_error(errno, std::system_category());
    }
  }

  void appendFront(const void *buffer, uint32_t size) {
    assert(size <= free());
    memcpy(m_buffer + m_head_index,  buffer, size);
    m_head_index += size;
  }

  void appendBack(const void *buffer, uint32_t size) {
    assert(size <= free());
    memcpy(m_buffer + m_tail_index - size + 1, buffer, size);
    m_tail_index -= size;
  }

  void delete_from_fs() {
    delete_file(m_filename);
  }

  const char *data() {
    return m_buffer;
  }

  uint32_t size() {
    return m_size;
  }

  uint32_t head_index() {
    return m_head_index;
  }

  uint32_t tail_index() {
    return m_tail_index;
  }

private:
  std::string m_filename;
  char *m_buffer = nullptr;
  uint32_t m_head_index = 0;
  uint32_t m_tail_index = 0;
  uint32_t m_size;

  uint32_t free() {
    return m_tail_index - m_head_index + 1;
  }
};

#endif
