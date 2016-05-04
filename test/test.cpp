#define CATCH_CONFIG_MAIN

#include <random>
#include <iostream>
#include <cassert>
#include <string>
#include <cstring>
#include <chrono>
#include <thread>

#include "catch.hpp"
#include "Config.hpp"
#include "Buffer.hpp"
#include "AppendableMMap.hpp"
#include "TableBuilder.hpp"
#include "LSMTree.hpp"
#include "KVStore.hpp"
#include "ParallelKVStore.hpp"
#include "Utils.hpp"

using namespace std;

shared_ptr<Table> create_table(uint32_t size, const vector<tuple<string, string>> &kv) {
  auto builder = TableBuilder(size);
  for (const auto &item : kv) {
    REQUIRE(builder.add(get<0>(item), get<1>(item)));
  }
  auto table = builder.finalize();
  REQUIRE(table->size() == kv.size());
  return table;
}

TEST_CASE( "Buffer" ) {
  char data[] = "Hello World!";

  SECTION( "String initialization" ) {
    string test = data;
    auto buffer = Buffer(test);
    REQUIRE( strncmp(test.c_str(), buffer.data(), test.size()) == 0 );
  }

  SECTION( "Serialization" ) {
    char test[100];
    uint16_t size = sizeof(data);
    *((uint16_t *)test) = size;
    strcpy(test + sizeof(uint16_t), data);

    auto buffer = Buffer::deserialize(test);
    REQUIRE( buffer.size() == size);
    REQUIRE( memcmp(buffer.data(), data, size) == 0 );
  }

  SECTION( "Deserialization" ) {
    string test = data;
    auto buffer = Buffer(test);
    AppendableMMap tmp(100);
    buffer.serialize(tmp);
    auto result = Buffer::deserialize(tmp.data());
    REQUIRE( buffer == result );
  }
}

TEST_CASE( "AppendableMMap" ) {
  string filename = "foobar";

  SECTION( "Write" ) {
    remove(filename.c_str());
    AppendableMMap map(filename.size(), filename.c_str());
    map.appendBack(filename.c_str(), filename.size());
  }

  SECTION( "Read" ) {
    AppendableMMap map(filename);
    REQUIRE(map.size() == filename.size());
    REQUIRE(strncmp(filename.c_str(), map.data(), filename.size()) == 0);
    remove(filename.c_str());
  }
}

TEST_CASE( "Table" ) {
  auto kv = create_random_kv(100000);
  auto table = create_table(1 << 23, kv);

  SECTION( "Build" ) {
    REQUIRE( table->size() == kv.size() );

    int i = 0;
    for (const auto &item : *table) {
      REQUIRE( item.key == Buffer(get<0>(kv[i])));
      REQUIRE( item.value == Buffer(get<1>(kv[i])));
      i++;
    }
  }

  SECTION( "Find value" ) {
    for (const auto &item : kv) {
      auto value = table->get(get<0>(item));
      REQUIRE (value != nullptr );
      REQUIRE (*value == Buffer(get<1>(item)));
    }

    auto value = table->get("{}");
    REQUIRE (value == nullptr);
  }

  SECTION( "Benchmark" ) {
    random_shuffle(kv.begin(), kv.end());
    auto n = 2000000 / kv.size();
    auto start = chrono::steady_clock::now();

    for (int i = 0; i < n; i++) {
      for (const auto &item : kv) {
        auto value = table->get(get<0>(item));
      }
    }

    auto end = chrono::steady_clock::now();
    auto diff = end - start;
    auto time = chrono::duration <float> (diff).count();
    cout << "Table gets (entries/sec): " << n*kv.size()/time << endl;
  }
}

TEST_CASE( "Table merging" ) {
  uint32_t kv_size = 1000;
  uint32_t table_size = 1 << 20;
  uint32_t num_tables = 13;

  vector<shared_ptr<Table>> tables;
  map<string, string> reference;

  for (int i = 0; i < num_tables; i++) {
    auto kv = create_random_kv(kv_size, (i % 2 == 0));
    auto table = create_table(table_size, kv);

    tables.push_back(table);

    for (const auto &item : kv) {
      reference[get<0>(item)] = get<1>(item);
    }
  }

  reverse(tables.begin(), tables.end());
  LevelConfig config;
  config.table_size = 1 << 20;
  auto merged_tables = TableBuilder::merge_tables(tables, config);

  SECTION( "Size" ) {
    uint32_t total_size = 0;
    for (const auto &table : merged_tables) {
      total_size += table->size();
    }

    REQUIRE(total_size == reference.size());
  }

  SECTION( "Order" ) {
    auto ref_it = reference.begin();
    Buffer last_min = merged_tables[0]->min_key();

    for (const auto &table : merged_tables) {
      auto table_it = table->begin();

      Buffer tmp = table->min_key();
      REQUIRE(tmp >= last_min);
      last_min = tmp;

      for (const auto &item : *table) {
        auto ref_key = get<0>(*ref_it);
        auto ref_value = get<1>(*ref_it);

        REQUIRE(item.key == ref_key);
        REQUIRE(item.value == ref_value);
        ref_it++;
      }
    }
  }

  SECTION( "Benchmark" ) {
    uint32_t total_size = 0;
    for (const auto &table : tables) {
      total_size += table->size();
    }
    auto n = 2000000 / total_size;

    auto start = chrono::steady_clock::now();
    for (int i = 0; i < n; i++) {
      auto merged_tables = TableBuilder::merge_tables(tables, config);
    }

    auto end = chrono::steady_clock::now();
    auto diff = end - start;
    auto time = chrono::duration <float> (diff).count();
    cout << "Merging (entries/sec): " << n*total_size/time << endl;
  }
}

TEST_CASE( "Level" ) {
  auto t = system("rm -rf /tmp/db");

  MemTable table1, table2, table3, table4;
  table1.add("a", "a");
  table2.add("b", "b");
  table3.add("c", "c");
  table4.add("a", "y");

  // Dump memtables to level 0
  LevelConfig config0("/tmp", "db", 0, 28, 1);
  auto level0 = make_shared<Level0>(config0);
  level0->dump_memtable(table1);
  level0->dump_memtable(table2);
  level0->dump_memtable(table3);
  level0->dump_memtable(table4);
  REQUIRE(level0->size() == 4);

  // Verify that values with the same key are shadowed correctly
  auto value = level0->get("a");
  REQUIRE(value != nullptr);
  REQUIRE(*value == "y");

  // Merge level 0 with level 1
  LevelConfig config1("/tmp", "db", 1, 14, 1);
  auto level1 = make_shared<LevelN>(config1);
  level1->merge_with(level0);
  REQUIRE(level0->size() == 0);
  REQUIRE(level1->size() == 3);

  // Verify that values with the same key are overwritten correctly
  value = level1->get("a");
  REQUIRE(value != nullptr);
  REQUIRE(*value == "y");

  // Merge with overlapping table
  MemTable table5;
  table5.add("b", "z");
  level0->dump_memtable(table5);
  level1->merge_with(level0);
  REQUIRE(*(level1->get("b")) == "z");
  REQUIRE(level0->size() == 0);
  REQUIRE(level1->size() == 3);
}

TEST_CASE( "LSMTree" ) {
  Config config("db", "/tmp/", 4, 1 << 10, 2, 1024);
  auto t = system("rm -rf /tmp/db");

  SECTION( "Fill" ) {
    LSMTree tree(config);
    for (int i = 0; i < 10; i++) {
      auto kv = create_random_kv(1000, false, 5);
      tree.dump_memtable(kv);
      for (const auto &item : kv) {
        auto value = tree.get(get<0>(item));
        REQUIRE (value != nullptr );
        REQUIRE (*value == Buffer(get<1>(item)));
      }
    }

    cout << tree;
    tree.destroy();
  }

  SECTION( "Load" ) {
    auto kv1 = create_random_kv(1000, false, 8);
    auto kv2 = create_random_kv(1000, true, 8);

    {
      LSMTree tree(config);
      tree.dump_memtable(kv1);
      tree.dump_memtable(kv2);
    }

    LSMTree other(config);
    for (const auto &item : kv2) {
      auto value = other.get(get<0>(item));
      REQUIRE (value != nullptr );
      REQUIRE (*value == Buffer(get<1>(item)));
    }

    other.destroy();
  }
}

TEST_CASE( "KVStore" ) {
  auto t = system("rm -rf /tmp/db");

  Config config("db", "/tmp/", 4, 1 << 10, 17, 1024);
  auto *store = new KVStore(config);

  // Add
  store->add("foo", "bar");
  auto res = store->get("foo");
  REQUIRE(res != nullptr);
  REQUIRE(*res == "bar");

  // Remove
  store->remove("foo");
  res = store->get("foo");
  REQUIRE(res == nullptr);

  // Restore saved version
  store->add("foo", "bar");
  delete store;
  store = new KVStore(config);
  res = store->get("foo");
  REQUIRE(res != nullptr);
  REQUIRE(*res == "bar");

  // Deletion
  store->destroy();
  delete store;
  REQUIRE(system("ls /tmp/db > /dev/null 2>&1") != 0);
}

TEST_CASE( "ParallelKVStore" ) {
  auto t = system("rm -rf /tmp/db*");
  auto num_cores = std::thread::hardware_concurrency();

  SECTION("Basic") {
    Config config("db", "/tmp/", 4, 1 << 23, 17, 1 << 20, 2);

    // Add & Get
    auto store = new ParallelKVStore(config);
    store->add("foo", "bar");
    delete store;

    // Restore
    store = new ParallelKVStore(config);
    auto future = store->get("foo");
    auto res = future.get();
    REQUIRE(*res == "bar");

    // Remove
    store->remove("foo");
    future = store->get("foo");
    res = future.get();
    REQUIRE(res == nullptr);

    // Destroy
    store->destroy();
    delete store;
    REQUIRE(system("ls /tmp/db* > /dev/null 2>&1") != 0);
  }

  SECTION( "Single Client ") {
    Config config("db", "/tmp/", 4, 1 << 23, 17, 1 << 20, 8);
    vector<tuple<string, string>> kv;
    map<string, string> truth;
    tie(kv, truth) = create_random_data(10000, false, 16);
    REQUIRE(kv.size() != truth.size());

    // Write
    auto store = new ParallelKVStore(config);
    for (const auto &item : kv) {
      store->add(get<0>(item), get<1>(item));
    }
    delete store;

    // Read back
    store = new ParallelKVStore(config);
    for (const auto &item : truth) {
      auto value = store->get(item.first).get();
      REQUIRE(*value == item.second);
    }

    store->destroy();
    delete store;
  }

  SECTION( "Multiple Clients Read Benchmark" ) {
    for (int cores = 1; cores <= num_cores/2; cores <<= 1) {
      Config config("db", "/tmp/", 4, 1 << 23, 17, 1 << 20, cores);
      auto tmp = get<1>(create_random_data(100000, false, 16));
      vector<pair<string, string>> kv(tmp.begin(), tmp.end());

      // Write data
      auto store = new ParallelKVStore(config);
      for (const auto &item : kv) {
        store->add(get<0>(item), get<1>(item));
      }
      delete store;
      auto t = system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"); // flush page cache

      // Read it back
      store = new ParallelKVStore(config);
      auto start = chrono::steady_clock::now();

      vector<thread> threads;
      int nthreads = num_cores/2;
      for (int i = 0; i < nthreads; i++) {
        threads.push_back(thread([&kv, &store, i, nthreads]{
          auto chunk_size = kv.size()/nthreads;
          auto offset = i * chunk_size;

          vector<future<shared_ptr<Buffer>>> results;
          results.reserve(chunk_size);

          for (auto j = offset; j < offset + chunk_size; j++) {
            const auto &item = kv[j];
            results.push_back(move(store->get(get<0>(item))));
          }

          for (auto &res : results) {
            // REQUIRE is not threadsafe...
            auto value = res.get();
            assert(value != nullptr);
          }
        }));
      }

      for (auto &thread : threads) {
        thread.join();
      }

      delete store;

      auto end = chrono::steady_clock::now();
      auto diff = end - start;
      auto time = chrono::duration <float> (diff).count();
      cout << "Gets (entries/sec) with " << cores << " cores: " << kv.size()/time << endl;

      t = system("rm -rf /tmp/db*");
    }
  }

  SECTION( "Multiple Clients Write Benchmark" ) {
    auto kv = get<0>(create_random_data(200000, false, 16));

    for (int cores = 1; cores <= num_cores/2; cores <<= 1) {
      Config config("db", "/tmp/", 4, 1 << 23, 17, 1 << 20, cores);
      auto store = new ParallelKVStore(config);
      auto start = chrono::steady_clock::now();

      vector<thread> threads;
      int nthreads = num_cores/2;
      for (int i = 0; i < nthreads; i++) {
        threads.push_back(thread([&kv, &store, i, nthreads]{
          auto chunk_size = kv.size()/nthreads;
          auto offset = i * chunk_size;

          for (auto j = offset; j < offset + chunk_size; j++) {
            const auto &item = kv[j];
            store->add(get<0>(item), get<1>(item));
          }
        }));
      }

      for (auto &thread : threads) {
        thread.join();
      }

      delete store;

      auto end = chrono::steady_clock::now();
      auto diff = end - start;
      auto time = chrono::duration <float> (diff).count();
      cout << "Store adds (entries/sec) with " << cores << " cores: " << time << " " << kv.size()/time << endl;
      auto t = system("rm -rf /tmp/db*");
    }
  }
}
