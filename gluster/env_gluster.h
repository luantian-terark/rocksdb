//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <algorithm>
#include <cstdio>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include "util/threadpool_imp.h"
extern "C" {
#include <glusterfs/api/glfs.h>
}
#ifdef USE_GLUSTER
namespace rocksdb {

// TODO check exception for gluster

//
// The Gluster environment for rocksdb. This class overrides all the
// file/dir access methods and thread-mgmt methods
//
class GlusterEnv : public Env {
 public:
  GlusterEnv(const std::string& server, const std::string& vol,
             const std::string& mount_point);
  virtual ~GlusterEnv();
  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  Status NewSequentialFile(const std::string& filename,
                           unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  Status NewRandomAccessFile(const std::string& filename,
                             unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& env) override;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  Status NewWritableFile(const std::string& filename,
                         unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  Status NewDirectory(const std::string& name,
                      unique_ptr<Directory>* result) override;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  Status FileExists(const std::string& filename) override;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* result) override;

  // Delete the named file.
  Status DeleteFile(const std::string& filename) override;

  // Create the specified directory. Returns error if directory exists.
  Status CreateDir(const std::string& dirname) override;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  Status CreateDirIfMissing(const std::string& dirname) override;

  // Delete the specified directory.
  Status DeleteDir(const std::string& dirname) override;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override;

  // Store the size of filename in *file_size.
  Status GetFileSize(const std::string& filename, uint64_t* file_size) override;

  // Store the last modification time of fname in *file_mtime.
  Status GetFileModificationTime(const std::string& filename,
                                 uint64_t* mtime) override;

  // Rename file src to target.
  Status RenameFile(const std::string& src, const std::string& target) override;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  Status LockFile(const std::string& filename, FileLock** lock) override;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  Status UnlockFile(FileLock* lock) override;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  Status GetTestDirectory(std::string* path) override;

  // Create and return a log file for storing informational messages.
  Status NewLogger(const std::string& filename,
                   shared_ptr<Logger>* result) override;

  // Get full directory name for this db.
  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override;

  // Truncate the named file to the specified size.
  Status Truncate(const std::string& filename, size_t size) override;
  static uint64_t gettid() {
    assert(sizeof(pthread_t) <= sizeof(uint64_t));
    return static_cast<uint64_t>(pthread_self());
  }

  // Opens `filename` as a memory-mapped file for read and write (in-place
  // updates only, i.e., no appends). On success, stores a raw buffer
  // covering the whole file in `*result`. The file must exist prior to this
  // call.
  Status NewMemoryMappedFileBuffer(
      const std::string& filename,
      unique_ptr<MemoryMappedFileBuffer>* result) override;
  ////////////////////////////////////////////////////////////////////////////////
  Status NewRandomRWFile(const std::string&, unique_ptr<RandomRWFile>*,
                         const EnvOptions&) override;
  Status LinkFile(const std::string&, const std::string&) override;
  Status NumFileLinks(const std::string&, uint64_t*) override;
  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override;
  Status ReopenWritableFile(const std::string& fname,
                            unique_ptr<WritableFile>* result,
                            const EnvOptions&) override;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;
  Status GetFreeSpace(const std::string&, uint64_t*) override;
  Status SetAllowNonOwnerAccess(bool) override;

 private:
  ////////////////////////////////////////////////////////////////////////////////
  // copied from poxix_env, these may be changed to std::thread later
  ////////////////////////////////////////////////////////////////////////////////
  std::string mount_point_;
  Env* default_env_;
  bool allow_non_owner_access_ = true;
  glfs_t* fs_{};
  // Returns full path from system root to the file through mounting path.
  std::string GenerateMountPath(const std::string& filename) const;

 public:
  ////////////////////////////////////////////////////////////////////////////////
  // transmit to default_env_
  ////////////////////////////////////////////////////////////////////////////////
  void Schedule(void (*function)(void* arg), void* arg, Priority pri, void* tag,
                void (*unschedFunction)(void* arg)) override;
  void StartThread(void (*function)(void* arg), void* arg) override;
  void SetBackgroundThreads(int number, Priority pri) override;
  int GetBackgroundThreads(Priority pri) override;
  void IncBackgroundThreadsIfNeeded(int number, Priority pri) override;
  int UnSchedule(void* arg, Priority pri) override;
  void WaitForJoin() override;
  unsigned GetThreadPoolQueueLen(Priority pri) const override;
  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override;
  uint64_t GetThreadID() const override;
  ThreadStatusUpdater* GetThreadStatusUpdater() const override;

  void LowerThreadPoolIOPriority(Priority pir) override;
  void LowerThreadPoolCPUPriority(Priority pri) override;

  std::string GenerateUniqueId() override;
  std::string TimeToString(uint64_t time) override;

  uint64_t NowMicros() override;
  void SleepForMicroseconds(int micros) override;
  Status GetHostName(char* name, uint64_t len) override;
  Status GetCurrentTime(int64_t* unix_time) override;

  uint64_t NowNanos() override;

  ////////////////////////////////////////////////////////////////////////////////
  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override;
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override;
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override;
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override;
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override;
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override;
  ////////////////////////////////////////////////////////////////////////////////
};

}  // namespace rocksdb

#else  // USE_GLUSTER

namespace rocksdb {

class GlusterEnv : public Env {
 public:
  explicit GlusterEnv() {
    fprintf(stderr, "You have not build rocksdb with Gfapi support\n");
    fprintf(stderr, "Please see gluster/README for details\n");
    abort();
  }

  virtual ~GlusterEnv() {}

  virtual Status NewSequentialFile(const std::string& filename,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& /*filename*/,
                                     unique_ptr<RandomAccessFile>* /*result*/,
                                     const EnvOptions& /*options*/) override {
    return Status::NotSupported();
  }

  virtual Status NewWritableFile(const std::string& /*filename*/,
                                 unique_ptr<WritableFile>* /*result*/,
                                 const EnvOptions& /*options*/) override {
    return Status::NotSupported();
  }

  virtual Status NewDirectory(const std::string& /*name*/,
                              unique_ptr<Directory>* /*result*/) override {
    return Status::NotSupported();
  }

  virtual Status FileExists(const std::string& /*filename*/) override {
    return Status::NotSupported();
  }

  virtual Status GetChildren(const std::string& /*path*/,
                             std::vector<std::string>* /*result*/) override {
    return Status::NotSupported();
  }

  virtual Status DeleteFile(const std::string& /*filename*/) override {
    return Status::NotSupported();
  }

  virtual Status CreateDir(const std::string& /*name*/) override {
    return Status::NotSupported();
  }

  virtual Status CreateDirIfMissing(const std::string& /*name*/) override {
    return Status::NotSupported();
  }

  virtual Status DeleteDir(const std::string& /*name*/) override {
    return Status::NotSupported();
  }

  virtual Status GetFileSize(const std::string& /*filename*/,
                             uint64_t* /*size*/) override {
    return Status::NotSupported();
  }

  virtual Status GetFileModificationTime(const std::string& /*filename*/,
                                         uint64_t* /*time*/) override {
    return Status::NotSupported();
  }

  virtual Status RenameFile(const std::string& /*src*/,
                            const std::string& /*target*/) override {
    return Status::NotSupported();
  }

  virtual Status LinkFile(const std::string& /*src*/,
                          const std::string& /*target*/) override {
    return Status::NotSupported();
  }

  virtual Status LockFile(const std::string& /*filename*/,
                          FileLock** /*lock*/) override {
    return Status::NotSupported();
  }

  virtual Status UnlockFile(FileLock* /*lock*/) override {
    return Status::NotSupported();
  }

  virtual Status NewLogger(const std::string& /*filename*/,
                           shared_ptr<Logger>* /*result*/) override {
    return Status::NotSupported();
  }

  virtual void Schedule(void (*/*function*/)(void* arg), void* /*arg*/,
                        Priority /*pri*/ = LOW, void* /*tag*/ = nullptr,
                        void (*/*unschedFunction*/)(void* arg) = 0) override {}

  virtual int UnSchedule(void* /*tag*/, Priority /*pri*/) override { return 0; }

  virtual void StartThread(void (*/*function*/)(void* arg),
                           void* /*arg*/) override {}

  virtual void WaitForJoin() override {}

  virtual unsigned int GetThreadPoolQueueLen(
      Priority /*pri*/ = LOW) const override {
    return 0;
  }

  virtual Status GetTestDirectory(std::string* /*path*/) override {
    return Status::NotSupported();
  }

  virtual uint64_t NowMicros() override { return 0; }

  virtual void SleepForMicroseconds(int /*micros*/) override {}

  virtual Status GetHostName(char* /*name*/, uint64_t /*len*/) override {
    return Status::NotSupported();
  }

  virtual Status GetCurrentTime(int64_t* /*unix_time*/) override {
    return Status::NotSupported();
  }

  virtual Status GetAbsolutePath(const std::string& /*db_path*/,
                                 std::string* /*outputpath*/) override {
    return Status::NotSupported();
  }

  virtual void SetBackgroundThreads(int /*number*/,
                                    Priority /*pri*/ = LOW) override {}
  virtual int GetBackgroundThreads(Priority /*pri*/ = LOW) override {
    return 0;
  }
  virtual void IncBackgroundThreadsIfNeeded(int /*number*/,
                                            Priority /*pri*/) override {}
  virtual std::string TimeToString(uint64_t /*number*/) override { return ""; }

  virtual uint64_t GetThreadID() const override { return 0; }
};
}  // namespace rocksdb

#endif  // WITH_GLUSTER
