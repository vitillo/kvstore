#include <string>
#include <cstdlib>
#include <vector>
#include <map>
#include <set>
#include <tuple>
#include <algorithm>

class LCG {
public:
  LCG(long seed = -1) {
    if (seed = -1) {
      srand(time(NULL));
      next = ::rand();
    } else {
      next = seed;
    }
  }

  int rand() {
    next = next * 1103515245 + 12345;
    return (unsigned int)(next/65536) % 32768;
  }

  void reseed(unsigned long seed) {
    next = seed;
  }

  unsigned long getseed() {
    return next;
  }

private:
  unsigned long next;
};

static LCG generator;

std::string gen_random(bool biased = false, int max_len = 16, int min_len = 1) {
  static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  int diff = max_len - min_len;
  int len = (diff != 0) ? (min_len + (generator.rand() % diff)) : min_len;
  char s[len];

  for (int i = 0; i < len; ++i) {
    if (biased) {
      s[i] = alphanum[generator.rand() % 10];
    } else {
      s[i] = alphanum[generator.rand() % (sizeof(alphanum) - 1)];
    }
  }

  s[len] = 0;
  return std::string(s);
}

std::tuple<std::vector<std::tuple<std::string, std::string>>, std::map<std::string, std::string>>
create_random_data(uint32_t num_entries, bool biased = false, int max_len = 16, int min_len = 1) {
  std::vector<std::tuple<std::string, std::string>> kv;
  std::map<std::string, std::string> truth;

  while (num_entries != kv.size()) {
    auto key = gen_random(biased, max_len, min_len);
    auto value = gen_random(biased, max_len, min_len);
    kv.push_back(make_tuple(key, value));
    truth[key] = value;
  }

  return make_tuple(kv, truth);
}


std::tuple<std::vector<std::tuple<std::string, std::string>>, std::map<std::string, std::string>>
create_skewed_random_data(uint32_t num_entries, int unique_elems, int len = 16) {
  return create_random_data(unique_elems, false, len, len);
}

std::vector<std::tuple<std::string, std::string>> create_random_kv(uint32_t num_entries, bool biased = false, int max_len = 16) {
  std::vector<std::tuple<std::string, std::string>> kv;
  for (int i = 0; i < num_entries; ++i) {
    auto key = gen_random(biased, max_len);
    auto value = gen_random(biased, max_len);
    kv.push_back(make_tuple(key, value));
  }

  std::sort(kv.begin(), kv.end());
  auto it = std::unique(kv.begin(), kv.end(), [](std::tuple<std::string, std::string> a, std::tuple<std::string, std::string>  b) { return std::get<0>(a) == std::get<0>(b); });
  kv.resize(std::distance(kv.begin(),it));

  return kv;
}
