//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "util/filename.h"

#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace {

struct TableReaderPtrHolder {
  std::unique_ptr<TableReader> ptr;
  std::mutex  mtx;
  std::condition_variable cond;
  Status open_status = Status::OK();
};

template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static void DeleteTableReader(void* arg1, void* arg2) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  delete table_reader;
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

#ifndef ROCKSDB_LITE

void AppendVarint64(IterKey* key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint64(buf, v);
  key->TrimAppend(key->Size(), buf, ptr - buf);
}

#endif  // ROCKSDB_LITE

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions), env_options_(env_options), cache_(cache) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {
}

void TableCache::CloseTables(void* ptr, size_t) {
  TableReaderPtrHolder *holder = reinterpret_cast<TableReaderPtrHolder *>(ptr);
  if (holder->ptr) {
    holder->ptr->Close();
  }
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, unique_ptr<TableReader>* table_reader,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache,
    bool for_compaction) {
  std::string fname =
      TableFileName(ioptions_.db_paths, fd.GetNumber(), fd.GetPathId());
  unique_ptr<RandomAccessFile> file;
  Status s = ioptions_.env->NewRandomAccessFile(fname, &file, env_options);

  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.ok()) {
    if (readahead > 0) {
      file = NewReadaheadRandomAccessFile(std::move(file), readahead);
    }
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }
    StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.env,
            record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
            file_read_hist, ioptions_.rate_limiter, for_compaction));
    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, env_options, internal_comparator,
                           skip_filters, level),
        std::move(file_reader), fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             TableReader** pp_table,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, bool skip_filters,
                             int level,
                             bool prefetch_index_and_filter_in_cache) {
  PERF_TIMER_GUARD(find_table_nanos);
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));
  assert(nullptr != pp_table);
  *pp_table = nullptr;
  TableReaderPtrHolder* existing = nullptr;
  if (*handle == nullptr) {
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    std::unique_ptr<TableReaderPtrHolder> holder(new TableReaderPtrHolder);
    auto deleter = &DeleteEntry<TableReaderPtrHolder>;
    s = cache_->Insert(key, holder.get(), 1, deleter, handle,
        Cache::Priority::LOW, (void**)&existing);
    if (!s.ok()) {
      return s;
    }
    if (existing) {
      assert(nullptr != *handle);
      goto HasExisting;
    }
    std::unique_lock<std::mutex> lock(holder->mtx);
    s = GetTableReader(env_options, internal_comparator, fd,
                       false /* sequential mode */, 0 /* readahead */,
                       record_read_stats, file_read_hist, &holder->ptr,
                       skip_filters, level, prefetch_index_and_filter_in_cache);
    if (!s.ok()) {
      assert(nullptr == holder->ptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
      holder->open_status = s;
    }
    *pp_table = holder->ptr.get();
    holder->cond.notify_all();
    // Release ownership of table reader.
    holder.release();
  }
  else {
    existing = reinterpret_cast<TableReaderPtrHolder*>(cache_->Value(*handle));
    assert(nullptr != existing);
  HasExisting:
    if (!existing->ptr) { // double check
      std::unique_lock<std::mutex> lock(existing->mtx);
      while (!existing->ptr) {
        if (!existing->open_status.ok()) {
          s = existing->open_status;
          cache_->Release(*handle);
          *handle = NULL; // holder will also be destroyed
          return s;
        }
        existing->cond.wait(lock);
      }
    }
    *pp_table = existing->ptr.get();
  }
  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileMetaData& meta,
    RangeDelAggregator* range_del_agg, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level, bool ignore_partial_remove) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  auto& fd = meta.fd;
  Status s;
  bool create_new_table_reader = false;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  size_t readahead = 0;
  if (for_compaction) {
#ifndef NDEBUG
    bool use_direct_reads_for_compaction = env_options.use_direct_reads;
    TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator:for_compaction",
                             &use_direct_reads_for_compaction);
#endif  // !NDEBUG
    if (ioptions_.new_table_reader_for_compaction_inputs) {
      // get compaction_readahead_size from env_options allows us to set the
      // value dynamically
      readahead = env_options.compaction_readahead_size;
      create_new_table_reader = true;
    }
  } else {
    readahead = options.readahead_size;
    create_new_table_reader = readahead > 0;
  }

  if (create_new_table_reader) {
    unique_ptr<TableReader> table_reader_unique_ptr;
    s = GetTableReader(
        env_options, icomparator, fd, true /* sequential_mode */, readahead,
        !for_compaction /* record stats */, nullptr, &table_reader_unique_ptr,
        false /* skip_filters */, level,
        true /* prefetch_index_and_filter_in_cache */, for_compaction);
    if (s.ok()) {
      table_reader = table_reader_unique_ptr.release();
    }
  } else {
    table_reader = fd.table_reader;
    if (table_reader == nullptr) {
      s = FindTable(env_options, icomparator, fd, &handle, &table_reader,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    !for_compaction /* record read_stats */, file_read_hist,
                    skip_filters, level);
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (options.table_filter &&
        !options.table_filter(*table_reader->GetTableProperties())) {
      result = NewEmptyInternalIterator(arena);
    } else {
      result = table_reader->NewIterator(options, arena, skip_filters);
      if (!ignore_partial_remove && meta.partial_removed) {
        auto wrapper = NewRangeWrappedInternalIterator(
            result, icomparator, &meta.range_set, arena);
        wrapper->RegisterCleanup([](void* arg1, void* arg2) {
          auto iter = reinterpret_cast<InternalIterator*>(arg1);
          if (arg2) {
            iter->~InternalIterator();
          } else {
            delete iter;
          }
        }, result, arena);
        result = wrapper;
      }
    }
    if (create_new_table_reader) {
      assert(handle == nullptr);
      result->RegisterCleanup(&DeleteTableReader, table_reader, nullptr);
    } else if (handle != nullptr) {
      result->RegisterCleanup(&UnrefEntry, cache_, handle);
      handle = nullptr;  // prevent from releasing below
    }

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && range_del_agg != nullptr && !options.ignore_range_deletions) {
    std::unique_ptr<InternalIterator> range_del_iter(
        table_reader->NewRangeTombstoneIterator(options));
    if (range_del_iter != nullptr) {
      s = range_del_iter->status();
    }
    if (s.ok()) {
      s = range_del_agg->AddTombstones(std::move(range_del_iter));
    }
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator(s, arena);
  }
  return result;
}

InternalIterator* TableCache::NewRangeTombstoneIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    HistogramImpl* file_read_hist, bool skip_filters, int level) {
  Status s;
  Cache::Handle* handle = nullptr;
  TableReader* table_reader = fd.table_reader;
  if (table_reader == nullptr) {
    s = FindTable(env_options, icomparator, fd, &handle, &table_reader,
                  options.read_tier == kBlockCacheTier /* no_io */,
                  true /* record read_stats */, file_read_hist, skip_filters,
                  level);
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    result = table_reader->NewRangeTombstoneIterator(options);
    if (result != nullptr) {
      if (handle != nullptr) {
        result->RegisterCleanup(&UnrefEntry, cache_, handle);
      }
    }
  }
  if (result == nullptr && handle != nullptr) {
    // the range deletion block didn't exist, or there was a failure between
    // getting handle and getting iterator.
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator(s);
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileMetaData& meta, const Slice& raw_k,
                       GetContext* get_context, HistogramImpl* file_read_hist,
                       bool skip_filters, int level) {
  const FileDescriptor& fd = meta.fd;
  Slice k = raw_k;
  while (meta.partial_removed) {
    auto find = std::upper_bound(
                    meta.range_set.begin(), meta.range_set.end(), k,
                    [&](const Slice& l, const InternalKey& r) {
                        return internal_comparator.Compare(l, r.Encode()) < 0;
                    });
    if ((find - meta.range_set.begin()) % 2 == 0) {
      if (find != meta.range_set.end()) {
        // assert( Comp(k , find->internal_key() <= 0 )
        if (ExtractUserKey(k) == find->user_key()) {
          k = find->Encode();
          break;
        }
      }
      if (find == meta.range_set.begin() || find[-1].Encode() != k) {
        return Status::OK();
      }
    }
    break;
  }
  std::string* row_cache_entry = nullptr;
  bool done = false;
#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions_.row_cache && !get_context->NeedToReadSequence()) {
    uint64_t fd_number = fd.GetNumber();
    auto user_key = ExtractUserKey(k);
    // We use the user key as cache key instead of the internal key,
    // otherwise the whole cache would be invalidated every time the
    // sequence key increases. However, to support caching snapshot
    // reads, we append the sequence number (incremented by 1 to
    // distinguish from 0) only in this case.
    uint64_t seq_no =
        options.snapshot == nullptr ? 0 : 1 + GetInternalKeySeqno(k);

    // Compute row cache key.
    row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                             row_cache_id_.size());
    AppendVarint64(&row_cache_key, fd_number);
    AppendVarint64(&row_cache_key, seq_no);
    row_cache_key.TrimAppend(row_cache_key.Size(), user_key.data(),
                             user_key.size());

    if (auto row_handle =
            ioptions_.row_cache->Lookup(row_cache_key.GetUserKey())) {
      // Cleanable routine to release the cache entry
      Cleanable value_pinner;
      auto release_cache_entry_func = [](void* cache_to_clean,
                                         void* cache_handle) {
        ((Cache*)cache_to_clean)->Release((Cache::Handle*)cache_handle);
      };
      auto found_row_cache_entry = static_cast<const std::string*>(
          ioptions_.row_cache->Value(row_handle));
      // If it comes here value is located on the cache.
      // found_row_cache_entry points to the value on cache,
      // and value_pinner has cleanup procedure for the cached entry.
      // After replayGetContextLog() returns, get_context.pinnable_slice_
      // will point to cache entry buffer (or a copy based on that) and
      // cleanup routine under value_pinner will be delegated to
      // get_context.pinnable_slice_. Cache entry is released when
      // get_context.pinnable_slice_ is reset.
      value_pinner.RegisterCleanup(release_cache_entry_func,
                                   ioptions_.row_cache.get(), row_handle);
      replayGetContextLog(*found_row_cache_entry, user_key, get_context,
                          &value_pinner);
      RecordTick(ioptions_.statistics, ROW_CACHE_HIT);
      done = true;
    } else {
      // Not found, setting up the replay log.
      RecordTick(ioptions_.statistics, ROW_CACHE_MISS);
      row_cache_entry = &row_cache_entry_buffer;
    }
  }
#endif  // ROCKSDB_LITE
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (!done && s.ok()) {
    if (t == nullptr) {
      s = FindTable(env_options_, internal_comparator, fd, &handle, &t,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    true /* record_read_stats */, file_read_hist, skip_filters,
                    level);
    }
    if (s.ok() && get_context->range_del_agg() != nullptr &&
        !options.ignore_range_deletions) {
      std::unique_ptr<InternalIterator> range_del_iter(
          t->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        s = range_del_iter->status();
      }
      if (s.ok()) {
        s = get_context->range_del_agg()->AddTombstones(
            std::move(range_del_iter));
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = t->Get(options, k, get_context, skip_filters);
      get_context->SetReplayLog(nullptr);
    } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
      // Couldn't find Table in cache but treat as kFound if no_io set
      get_context->MarkKeyMayExist();
      s = Status::OK();
      done = true;
    }
  }

#ifndef ROCKSDB_LITE
  // Put the replay log in row cache only if something was found.
  if (!done && s.ok() && row_cache_entry && !row_cache_entry->empty()) {
    size_t charge =
        row_cache_key.Size() + row_cache_entry->size() + sizeof(std::string);
    void* row_ptr = new std::string(std::move(*row_cache_entry));
    ioptions_.row_cache->Insert(row_cache_key.GetUserKey(), row_ptr, charge,
                                &DeleteEntry<std::string>);
  }
#endif  // ROCKSDB_LITE

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  return s;
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* handle = nullptr;
  TableReader* table = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &handle, &table, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(handle);
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* handle = nullptr;
  TableReader* table = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &handle, &table, true);
  if (!s.ok()) {
    return 0;
  }
  assert(handle);
  auto ret = table->ApproximateMemoryUsage();
  ReleaseHandle(handle);
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

}  // namespace rocksdb
