#include <vector>
#include <iostream>
#include <unistd.h>
#include <string>
#include <atomic>
#include <limits>
#include <sstream>
#include <iomanip>
#include <vector>

#include "Utils.hpp"
#include "Config.hpp"
#include "ParallelKVStore.hpp"

using namespace std;

enum OP{
  NOP,
  FILLRANDOM,
  FILLSEQ,
  READRANDOM,
  READSEQ
};

int num_partitions = 1;
int num_threads = std::thread::hardware_concurrency()/4;
int threshold = 10;
int num_levels = 6;
int num_elements = 1 << 20;
int element_size = 1024;
int ss_table_size = 10 << 20;
int memtable_size = 10 << 20;
bool clear = true;
string path = "/tmp";

// See http://preshing.com/20121224/how-to-generate-a-sequence-of-unique-random-integers/
unsigned int permuteQPR_(unsigned int x) {
  static const unsigned int prime = 4294967291;
  if (x >= prime)
    return x;  // The 5 integers out of range are mapped to themselves.
  unsigned int residue = ((unsigned long long) x * x) % prime;
  return (x <= prime / 2) ? residue : prime - residue;
}

unsigned int permuteQPR(unsigned int x) {
  return permuteQPR_(permuteQPR_(x) ^ 0x5bf03635);
}

string pad(unsigned int x) {
  static auto padding = to_string(numeric_limits<unsigned int>::max()).size();
  stringstream ss;
  ss << setfill('0') << setw(padding) << x;
  return ss.str();
}

void read(const Config &config, bool random) {
  auto store = new ParallelKVStore(config);
  auto start = chrono::steady_clock::now();
  vector<thread> threads;
  std::atomic<long> bytes;

  bytes = 0;
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(thread([&store, i, &bytes, random]{
          auto chunk_size = num_elements/num_threads;
          auto offset = i * chunk_size;

          vector<future<shared_ptr<Buffer>>> results;
          results.reserve(chunk_size);

          for (auto j = offset; j < offset + chunk_size; j++) {
            auto key = random ? to_string(permuteQPR(j)) : pad(j);
            results.push_back(move(store->get(key)));
            bytes += key.size();
          }

          for (auto &res : results) {
            auto value = res.get();
            if (value) {
              bytes += value->size();

              if (i == 0 && bytes > threshold) {
                cout << "Total size: " << (bytes >> 20) << " MB" << endl;
                threshold += 1 << 28;
              }
            }
          }
        }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  delete store;
  auto end = chrono::steady_clock::now();
  auto diff = end - start;
  auto duration = chrono::duration <float> (diff).count();

  cout << "Total size: " << (bytes >> 20) << " MB" << endl;
  cout << "Duration: " << duration << " seconds" << endl;
  cout << "Read rate: " << (bytes >> 20)/duration << " MB/sec" << endl;
  cout << "Read rate: " << num_elements/duration << " items/sec" << endl;
}

void fill(const Config &config, bool random) {
  auto store = new ParallelKVStore(config);
  auto start = chrono::steady_clock::now();
  vector<thread> threads;
  std::atomic<long> bytes;

  bytes = 0;
  char value[element_size + 1] = {0};
  memset(value, 'F', element_size);

  for (int i = 0; i < num_threads; i++) {
    threads.push_back(thread([&store, i, &bytes, &value, random]{
          auto chunk_size = num_elements/num_threads;
          auto offset = i * chunk_size;
          long threshold = 1 << 28;

          for (long j = offset; j < offset + chunk_size; j++) {
            auto key = random ? pad(permuteQPR(j)) : pad(j);
            bytes += key.size() + element_size;
            store->add(key, value);

            if (i == 0 && bytes > threshold) {
              cout << "Total size: " << (bytes >> 20) << " MB" << endl;
              threshold += 1 << 28;
            }
          }
        }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  delete store;
  auto end = chrono::steady_clock::now();
  auto diff = end - start;
  auto duration = chrono::duration <float> (diff).count();

  cout << "Total size: " << (bytes >> 20) << " MB" << endl;
  cout << "Duration: " << duration << " seconds" << endl;
  cout << "Fill rate: " << (bytes >> 20)/duration << " MB/sec" << endl;
  cout << "Fill rate: " << num_elements/duration << " items/sec" << endl;
}

int main(int argc, char* argv[]) {
  OP op = NOP;
  int c;

  while ((c = getopt (argc, argv, "p:l:n:s:t:m:o:r:d:c:")) != -1) {
    switch (c) {
    case 'p':
      num_partitions = stoul(optarg);
      break;

    case 'c':
      clear = stoul(optarg);
      break;

    case 'r':
      num_threads = stoul(optarg);
      break;

    case 'l':
      num_levels = stoul(optarg);
      break;

    case 'n':
      num_elements = stoul(optarg);
      break;

    case 's':
      element_size = stoul(optarg);
      break;

    case 't':
      ss_table_size = stoul(optarg);
      break;

    case 'm':
      memtable_size = stoul(optarg);
      break;

    case 'd':
      path = optarg;
      break;

    case 'o':
      if (strcmp("fillrandom", optarg) == 0) {
        op = FILLRANDOM;
      } else if (strcmp("fillseq", optarg) == 0) {
        op = FILLSEQ;
      } else if (strcmp("readrandom", optarg) == 0) {
        op = READRANDOM;
      } else if (strcmp("readseq", optarg) == 0) {
        op = READSEQ;
      } else {
        cerr << "Invalid operation " << optarg << endl;
        return -1;
      }

      break;

    default:
      return -1;
    }
  }


  switch(op) {
  case FILLRANDOM:
    {
      Config config("db", path, num_levels, ss_table_size, threshold, memtable_size, num_partitions, clear);
      fill(config, true);
      break;
    }

  case FILLSEQ:
    {
      Config config("db", path, num_levels, ss_table_size, threshold, memtable_size, num_partitions, clear);
      fill(config, false);
      break;
    }

  case READRANDOM:
    {
      Config config("db", path, num_levels, ss_table_size, threshold, memtable_size, num_partitions);
      read(config, true);
      break;
    }

  case READSEQ:
    {
      Config config("db", path, num_levels, ss_table_size, threshold, memtable_size, num_partitions);
      read(config, false);
      break;
    }
  }

  return 0;
}
