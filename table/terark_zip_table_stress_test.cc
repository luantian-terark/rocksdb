//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>

#include "db/db_test_util.h"
#if _MSC_VER
# include "../../terark-zip-rocksdb/src/table/terark_zip_table.h"
#else
# include <table/terark_zip_weak_function.h>
#endif
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/perf_context.h"


#if _MSC_VER
std::string localTempDir = R"(C:\Users\ZZZ\AppData\Local\Temp)";
#else
std::string localTempDir = "/tmp";
#endif

namespace rocksdb {

std::string get_prefix_key(size_t i)
{
  char buffer[12];
  uint32_t p = (uint32_t)i % 4;
  if (port::kLittleEndian) {
    p = (uint32_t)EndianTransform(p, 4);
    i = EndianTransform(i, 8);
  }
  memcpy(buffer, &p, 4);
  memcpy(buffer + 4, &i, 8);
  return std::string(buffer, buffer + 12);
}

std::string get_value(size_t i)
{
  static std::string str = ([] {
    std::string s;
    for (int i = 0; i < 10; ++i) {
      s.append("0123456789QWERTYUIOPASDFGHJKLZXCVBNM");
    }
    return s;
  })();
  size_t pos = i % str.size();
  std::string value = get_prefix_key(i);
  value.append(str.data() + pos, str.size() - pos);
  return value;
}

class Rdb_pk_comparator : public Comparator {
public:
  Rdb_pk_comparator(const Rdb_pk_comparator &) = delete;
  Rdb_pk_comparator &operator=(const Rdb_pk_comparator &) = delete;
  Rdb_pk_comparator() = default;

  static int bytewise_compare(const rocksdb::Slice &a,
    const rocksdb::Slice &b) {
    const size_t a_size = a.size();
    const size_t b_size = b.size();
    const size_t len = (a_size < b_size) ? a_size : b_size;
    int res = memcmp(a.data(), b.data(), len);

    if (res)
      return res;

    /* Ok, res== 0 */
    if (a_size != b_size) {
      return a_size < b_size ? -1 : 1;
    }
    return 0;
  }

  /* Override virtual methods of interest */

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    return bytewise_compare(a, b);
  }

  const char *Name() const override { return "RocksDB_SE_v3.10"; }

  // TODO: advanced funcs:
  // - FindShortestSeparator
  // - FindShortSuccessor

  // for now, do-nothing implementations:
  void FindShortestSeparator(std::string *start,
    const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}
};

class Rdb_rev_comparator : public Comparator {
public:
  Rdb_rev_comparator(const Rdb_rev_comparator &) = delete;
  Rdb_rev_comparator &operator=(const Rdb_rev_comparator &) = delete;
  Rdb_rev_comparator() = default;

  static int bytewise_compare(const rocksdb::Slice &a,
    const rocksdb::Slice &b) {
    return -Rdb_pk_comparator::bytewise_compare(a, b);
  }

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    return -Rdb_pk_comparator::bytewise_compare(a, b);
  }
  const char *Name() const override { return "rev:RocksDB_SE_v3.10"; }
  void FindShortestSeparator(std::string *start,
    const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}
};

Rdb_pk_comparator pk_c;
Rdb_rev_comparator rev_c;

class TerarkZipStressTest : public DBTestBase {
public:
  TerarkZipStressTest() : DBTestBase("/terark_zip_reader_test") {}
};

TEST_F(TerarkZipStressTest, EndlessTest) {
  std::mt19937_64 mt;
  rocksdb::Options options = CurrentOptions();

  rocksdb::BlockBasedTableOptions bto;
  bto.block_size = 4 * 1024;
  bto.block_cache = std::shared_ptr<rocksdb::Cache>(rocksdb::NewLRUCache(8ULL * 1024 * 1024));
  rocksdb::TerarkZipTableOptions tzto0, tzto1;
  tzto0.localTempDir = localTempDir;
  tzto0.debugLevel = 2;
  tzto0.minDictZipValueSize = 0;
  tzto0.keyPrefixLen = 4;
  tzto0.keyPrefixLen = 0;
  tzto0.checksumLevel = 3;
  tzto0.offsetArrayBlockUnits = 128;
  tzto0.entropyAlgo = rocksdb::TerarkZipTableOptions::kFSE;
  tzto0.softZipWorkingMemLimit = 16ull << 30;
  tzto0.hardZipWorkingMemLimit = 32ull << 30;
  tzto0.smallTaskMemory = 1200 << 20; // 1.2G
  tzto1 = tzto0;
  tzto1.offsetArrayBlockUnits = 0;
  tzto1.entropyAlgo = rocksdb::TerarkZipTableOptions::kHuffman;
  auto table_factory0 = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewTerarkZipTableFactory(tzto0, rocksdb::NewBlockBasedTableFactory(bto)));
  auto table_factory1 = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewTerarkZipTableFactory(tzto1, rocksdb::NewBlockBasedTableFactory(bto)));
  options.allow_mmap_reads = true;
  options.compaction_pri = rocksdb::kOldestSmallestSeqFirst;
  options.comparator = &rev_c;
  options.allow_concurrent_memtable_write = true;
  options.max_open_files = 32768;

  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_multiplier = 8;
  options.disable_auto_compactions = false;
  options.num_levels = 20;
  options.max_subcompactions = 4;
  options.level_compaction_dynamic_level_bytes = true;
  options.compaction_options_universal.min_merge_width = 2;
  options.compaction_options_universal.max_merge_width = 4;
  options.compaction_options_universal.allow_trivial_move = true;
  options.compaction_options_universal.stop_style = rocksdb::kCompactionStopStyleSimilarSize;

  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 20;
  options.level0_stop_writes_trigger = 36;

  options.base_background_compactions = 4;
  options.max_background_compactions = 8;
  //options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(32ull << 10, 1000));

  std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
  {
    options.table_factory = table_factory0;
    options.enable_partial_remove = true;
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    options.write_buffer_size = options.target_file_size_base = 8ull << 20;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;
    options.memtable_factory.reset(rocksdb::NewThreadedRBTreeRepFactory());
    cfDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, options);

    options.table_factory = table_factory1;
    options.enable_partial_remove = true;
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.write_buffer_size = options.target_file_size_base = 8ull << 20;
    options.max_bytes_for_level_base = options.target_file_size_base * 4;
    options.memtable_factory.reset(new rocksdb::SkipListFactory);
    cfDescriptors.emplace_back("test", options);
  }

  options.env->SetBackgroundThreads(options.max_background_compactions, rocksdb::Env::LOW);
  options.env->SetBackgroundThreads(options.max_background_flushes, rocksdb::Env::HIGH);

  options.create_if_missing = true;

  rocksdb::DBOptions dbo = options;
  dbo.create_missing_column_families = true;
  rocksdb::Status s0, s1;
  std::vector<rocksdb::ColumnFamilyHandle*> hs;

  //rocksdb::DestroyDB("C:/osc/rocksdb_test/testdb", options);
  auto& db = db_;
  Destroy(last_options_);
  ASSERT_OK(rocksdb::DB::Open(dbo, dbname_, cfDescriptors, &hs, &db));
  last_options_ = options;
  rocksdb::ColumnFamilyHandle *h0 = hs[0], *h1 = hs[1];

  rocksdb::ReadOptions ro;
  rocksdb::WriteOptions wo;
  rocksdb::FlushOptions fo;

  std::random_device rd;
  mt.seed(rd());
  uint64_t count, stop = 1000000000;
  std::string key, value;
  std::string value_out0;
  std::string value_out1;
  std::unique_ptr<rocksdb::Iterator> iter0(db->NewIterator(ro, h0));
  std::unique_ptr<rocksdb::Iterator> iter1(db->NewIterator(ro, h1));
  std::vector<const rocksdb::Snapshot*> snapshot;

  rocksdb::CompactRangeOptions co;
  co.exclusive_manual_compaction = false;
  rocksdb::WriteBatchWithIndex b(&rev_c, 0, true, 0, "rbtree");
  for (count = 10000; count < 100000000; ++count)
  {
    std::uniform_int_distribution<uint64_t> uid(0, count);
    if (count % 1111111 == 0) {
      iter0.reset();
      iter1.reset();
      for (auto s : snapshot) {
        db->ReleaseSnapshot(s);
      }
      snapshot.clear();
      db->DestroyColumnFamilyHandle(h0);
      db->DestroyColumnFamilyHandle(h1);
      delete db;
      ASSERT_OK(rocksdb::DB::Open(dbo, dbname_, cfDescriptors, &hs, &db));
      h0 = hs[0];
      h1 = hs[1];
      ro.snapshot = nullptr;
      iter0.reset(db->NewIterator(ro, h0));
      iter1.reset(db->NewIterator(ro, h1));
    }
    if (count < stop) {
      size_t r = uid(mt);
      key = get_prefix_key(r);
      value = get_value(count);
      b.Put(h0, key, value);
      b.Put(h1, key, value);
      if (count % 4) {
        key = get_prefix_key(uid(mt));
        b.Delete(h0, key);
        b.Delete(h1, key);
      }
    }
    else {
      count = stop;
    }
    if (count % 23 == 0) {
      ASSERT_OK(db->Write(wo, b.GetWriteBatch()));
      b.Clear();
    }
    if (count % 103 == 0) {
      if (snapshot.size() < 2) {
        snapshot.emplace_back(db->GetSnapshot());
      }
      else if (snapshot.size() > 50 || (mt() & 1) == 0) {
        auto i = mt() % snapshot.size();
        db->ReleaseSnapshot(snapshot[i]);
        snapshot.erase(snapshot.begin() + i);
      }
      else {
        snapshot.emplace_back(db->GetSnapshot());
      }
    }
    for (int i = 0; i < 2; ++i) {
      size_t si = mt() % (snapshot.size() + 1);
      ro.snapshot = si == snapshot.size() ? nullptr : snapshot[si];
      key = get_prefix_key(uid(mt));
      s0 = b.GetFromBatchAndDB(db, ro, h0, key, &value_out0);
      s1 = b.GetFromBatchAndDB(db, ro, h1, key, &value_out1);
      if (s0.IsNotFound()) {
        ASSERT_TRUE(s1.IsNotFound());
      }
      else {
        ASSERT_EQ(value_out0, value_out1);
      }
    }
    if (count % 7 == 0) {
      size_t si = mt() % (snapshot.size() + 1);
      ro.snapshot = si == snapshot.size() ? nullptr : snapshot[si];
      iter0.reset(b.NewIteratorWithBase(h0, db->NewIterator(ro, h0)));
      iter1.reset(b.NewIteratorWithBase(h1, db->NewIterator(ro, h1)));
      key = get_prefix_key(uid(mt));
      iter0->Seek(key);
      iter1->Seek(key);
      if (iter0->Valid()) {
        ASSERT_EQ(iter0->key(), iter1->key());
      }
      else {
        ASSERT_FALSE(iter1->Valid());
      }
      key = get_prefix_key(uid(mt));
      iter0->SeekForPrev(key);
      iter1->SeekForPrev(key);
      if (iter0->Valid()) {
        ASSERT_EQ(iter0->key(), iter1->key());
      }
      else {
        ASSERT_FALSE(iter1->Valid());
      }
    }
  }
  //db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  
  system("pause");
  for (auto s : snapshot) {
    db->ReleaseSnapshot(s);
  }
  db->DestroyColumnFamilyHandle(h0);
  db->DestroyColumnFamilyHandle(h1);
  iter0.reset();
  iter1.reset();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  fprintf(stderr, "exit in 5 seconds\n");
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return ret;
}
