//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  
#pragma once
#include <algorithm>
#include <cstdio>
#include <ctime>

#include <iostream>
#include <set>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include <thread>

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

    private:
        glfs_t* fs_{};

    public:
        GlusterEnv(const std::string& vol, const std::string& server);
        virtual ~GlusterEnv();

        virtual Status NewSequentialFile(const std::string& fname,
            unique_ptr<SequentialFile>* result,
            const EnvOptions& options) override;

        virtual Status NewRandomAccessFile(const std::string& fname,
            unique_ptr<RandomAccessFile>* result,
            const EnvOptions& options) override;

        virtual Status NewWritableFile(const std::string& fname,
            unique_ptr<WritableFile>* result,
            const EnvOptions& options) override;

        virtual Status NewDirectory(const std::string& name,
            unique_ptr<Directory>* result) override;

        virtual Status FileExists(const std::string& fname) override;

        virtual Status GetChildren(const std::string& dir,
            std::vector<std::string>* result) override;

        virtual Status DeleteFile(const std::string& fname) override;

        virtual Status CreateDir(const std::string& dirname) override;

        virtual Status CreateDirIfMissing(const std::string& dirname) override;

        virtual Status DeleteDir(const std::string& dirname) override;

        virtual Status GetFileSize(const std::string& fname,
            uint64_t* file_size) override;

        virtual Status GetFileModificationTime(const std::string& fname,
            uint64_t* file_mtime) override;

        virtual Status RenameFile(const std::string& src,
            const std::string& target) override;

        virtual Status LockFile(const std::string& fname, FileLock** lock) override;

        virtual Status UnlockFile(FileLock* lock) override;

        virtual void Schedule(void(*function)(void* arg1), void* arg, Priority pri,
            void* tag, void(*unschedFunction)(void* arg)) override;

        virtual void StartThread(void(*function)(void* arg), void* arg) override;

        virtual Status GetTestDirectory(std::string* path) override;

        virtual Status NewLogger(const std::string& fname,
            shared_ptr<Logger>* result) override;

        virtual uint64_t NowMicros() override;

        virtual void SleepForMicroseconds(int micros) override;

        virtual Status GetHostName(char* name, uint64_t len) override;

        virtual Status GetCurrentTime(int64_t* unix_time) override;

        virtual Status GetAbsolutePath(const std::string& db_path,
            std::string* output_path) override;

        virtual void SetBackgroundThreads(int num, Priority pri) override;

        virtual int GetBackgroundThreads(Priority pri) override;

        virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override;

        virtual std::string TimeToString(uint64_t time) override;
        virtual Status Truncate(const std::string& filename, size_t size) override;
        static uint64_t gettid() {
            pthread_t tid = pthread_self();
            uint64_t thread_id = 0;
            memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
            return thread_id;
        }

    private:
        ////////////////////////////////////////////////////////////////////////////////
        // copied from poxix_env,  these may be changed to std::thread later
        ////////////////////////////////////////////////////////////////////////////////
        std::vector<ThreadPoolImpl> thread_pools_{};
        std::vector<pthread_t> threads_to_join_{};
        pthread_mutex_t mu_{};
        ////////////////////////////////////////////////////////////////////////////////
    };

}  // namespace rocksdb

#else  // USE_GLUSTER

namespace rocksdb {


    class GlusterEnv : public Env {
    public:
        template <typename...T>
        GlusterEnv(T... args) {
            fprintf(stderr, "You have not build rocksdb with Gfapi support\n");
            fprintf(stderr, "Please see gluster/README for details\n");
            abort();
        }

        virtual ~GlusterEnv() {}

        virtual Status NewSequentialFile(const std::string& fname,
            unique_ptr<SequentialFile>* result,
            const EnvOptions& options) override;

        virtual Status NewRandomAccessFile(const std::string& /*fname*/,
            unique_ptr<RandomAccessFile>* /*result*/,
            const EnvOptions& /*options*/) override {
            return Status::NotSupported();
        }

        virtual Status NewWritableFile(const std::string& /*fname*/,
            unique_ptr<WritableFile>* /*result*/,
            const EnvOptions& /*options*/) override {
            return Status::NotSupported();
        }

        virtual Status NewDirectory(const std::string& /*name*/,
            unique_ptr<Directory>* /*result*/) override {
            return Status::NotSupported();
        }

        virtual Status FileExists(const std::string& /*fname*/) override {
            return Status::NotSupported();
        }

        virtual Status GetChildren(const std::string& /*path*/,
            std::vector<std::string>* /*result*/) override {
            return Status::NotSupported();
        }

        virtual Status DeleteFile(const std::string& /*fname*/) override {
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

        virtual Status GetFileSize(const std::string& /*fname*/,
            uint64_t* /*size*/) override {
            return Status::NotSupported();
        }

        virtual Status GetFileModificationTime(const std::string& /*fname*/,
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

        virtual Status LockFile(const std::string& /*fname*/,
            FileLock** /*lock*/) override {
            return Status::NotSupported();
        }

        virtual Status UnlockFile(FileLock* /*lock*/) override { return Status::NotSupported(); }

        virtual Status NewLogger(const std::string& /*fname*/,
            shared_ptr<Logger>* /*result*/) override {
            return Status::NotSupported();
        }

        virtual void Schedule(void(*/*function*/)(void* arg), void* /*arg*/,
            Priority /*pri*/ = LOW, void* /*tag*/ = nullptr,
            void(*/*unschedFunction*/)(void* arg) = 0) override {}

        virtual int UnSchedule(void* /*tag*/, Priority /*pri*/) override { return 0; }

        virtual void StartThread(void(*/*function*/)(void* arg),
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
