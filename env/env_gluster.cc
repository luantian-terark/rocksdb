#ifdef USE_GLUSTER
#include <sys/stat.h>
#include <memory>
#include <mutex>

#include "gluster/env_gluster.h"
#include "monitoring/thread_status_updater.h"
#include "posix_logger.h"
#include "rocksdb/env.h"
#include "util/logging.h"

extern "C" {
#include <glusterfs/api/glfs-handles.h>
#include <glusterfs/api/glfs.h>
}
namespace rocksdb {
namespace {
////////////////////////////////////////////////////////////////////////////////
// Copy from posix_env
////////////////////////////////////////////////////////////////////////////////
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return nullptr;
}
int LockOrUnlock(glfs_fd_t* fd, bool lock) {
  errno = 0;
  struct flock f {};
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  const auto value = glfs_posix_lock(fd, F_SETLK, &f);

  return value;
}

class GfapiFlock : public FileLock {
 public:
  GfapiFlock(glfs_fd_t* fd, const std::string& filename)
      : fd_(fd), filename_(filename) {}
  virtual ~GfapiFlock() = default;
  glfs_fd_t* fd_ = nullptr;
  std::string filename_{};
};
struct {
 private:
  std::set<std::string> lockedFiles;
  std::mutex mtx;

 public:
  bool Insert(const std::string& filename) {
    std::lock_guard<std::mutex> l(mtx);
    return lockedFiles.insert(filename).second;
  }
  int Erase(const std::string& filename) {
    std::lock_guard<std::mutex> l(mtx);
    return lockedFiles.erase(filename);
  }
} lockedFileTable;

Status IOError(const std::string& context, int err_number) {
  return rocksdb::IOError(context, "", err_number);
}
using rocksdb::IOError;
////////////////////////////////////////////////////////////////////////////////

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
Logger* mylog = nullptr;
class GfapiDir : public Directory {
 public:
  ~GfapiDir();
  GfapiDir(const GfapiDir&) = delete;
  GfapiDir& operator=(const GfapiDir&) = delete;
  GfapiDir(glfs_fd_t* fd) : fd_(fd) {}
  Status Fsync() override { return Status::OK(); }

 public:
  glfs_fd_t* fd_;
};

class GfapiSequentialFile : public SequentialFile {
 public:
  GfapiSequentialFile(glfs_fd_t* fd, const std::string& filename)
      : fd_(fd), filename_(filename) {}
  Status Read(size_t n, Slice* result, char* scratch) override;
  Status Skip(uint64_t n) override;
  ~GfapiSequentialFile();

 private:
  glfs_fd_t* fd_;
  const std::string filename_{};
};

class GfapiRandomAccessFile : public RandomAccessFile {
 private:
  std::string filename_{};
  glfs_fd_t* fd_;

 public:
  GfapiRandomAccessFile(glfs_fd_t* fd, const std::string& filename)
      : filename_(filename), fd_(fd) {}

  virtual ~GfapiRandomAccessFile() {
    if (fd_) {
      glfs_close(fd_);
    }
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override;

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to each other by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  size_t GetUniqueId(char* id, size_t size) const override;
  // void Hint(AccessPattern) override;

  Status FsRead(uint64_t offset, size_t len, void* buf) const override;

 public:
  const std::string& getName() const { return filename_; }
};
////////////////////////////////////////////////////////////////////////////////
class GfapiWritableFile : public WritableFile {
 public:
  GfapiWritableFile(glfs_fd_t* fd, const std::string& filename);
  Status Append(const Slice& data) override;
  Status Close() override;
  Status Flush() override;
  Status Sync() override;
  Status Fsync() override;
  Status Truncate(uint64_t size) override;
  uint64_t GetFileSize() override { return filesize_; }
  ~GfapiWritableFile();
  GfapiWritableFile(const GfapiWritableFile&) = delete;

 private:
  glfs_fd_t* fd_;
  std::string filename_{};
  uint64_t filesize_ = 0;
  friend class GfapiLogger;
};

// The object that implements the debug logs to reside in Glugser.
class GfapiLogger : public Logger {
 private:
  unique_ptr<GfapiWritableFile> file_{};
  uint64_t (*gettid_)();  // Return the thread id for the current thread

 public:
  GfapiLogger(GfapiWritableFile* f, uint64_t (*gettid)());

  virtual ~GfapiLogger() {
    if (!closed_) {
      closed_ = true;
      ROCKS_LOG_DEBUG(mylog, "[gluster] GfapiLogger closed %s\n",
                      file_->filename_.c_str());
      file_->Close();
      file_->fd_ = nullptr;
      if (mylog != nullptr && mylog == this) {
        mylog = nullptr;
      }
    }
  }
  using Logger::Logv;  //
  void Logv(const char* format, va_list ap) override {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base = nullptr;
      int bufsize = 0;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t {};
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);

      file_->Append(Slice(base, p - base));
      file_->Flush();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};
}  // namespace

////////////////////////////////////////////////////////////////////////////////
// GlusterEnv member functions
////////////////////////////////////////////////////////////////////////////////

GlusterEnv::GlusterEnv(const std::string& vol, const std::string& server)
    : thread_pools_(Priority::TOTAL) {
  fs_ = glfs_new(vol.c_str());
  glfs_set_volfile_server(fs_, "tcp", server.c_str(), 24007);
  const auto ret = glfs_init(fs_);
  if (ret) {
    fprintf(stderr, "Failed to connect GlusterFS server/volume: %s/%s\n",
            server.c_str(), vol.c_str());
    exit(ret);
  }
  ThreadPoolImpl::PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  // ThreadPoolImpl::PthreadCall("cvar_init",
  //                            pthread_cond_init(&bgsignal_, nullptr));
}

GlusterEnv::~GlusterEnv() {
  fputs("Destroying GlusterEnv\n", stderr);
  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  for (auto pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].JoinAllThreads();
  }
  if (this != Env::Default()) {
    delete thread_status_updater_;
  }
}


Status GlusterEnv::NewSequentialFile(const std::string& fname,
                                     unique_ptr<SequentialFile>* result,
                                     const EnvOptions& /**/) {
  *result = nullptr;
  glfs_fd_t* fd = glfs_open(fs_, fname.c_str(), O_RDONLY);
  if (fd) {
    result->reset(new GfapiSequentialFile(fd, fname));
    return Status::OK();
  }

  result->reset(nullptr);
  return IOError(fname, errno);
}

Status GlusterEnv::NewRandomAccessFile(const std::string& fname,
                                       unique_ptr<RandomAccessFile>* result,
                                       const EnvOptions& /**/) {
  glfs_fd_t* fd = glfs_open(fs_, fname.c_str(), O_RDONLY);
  if (fd) {
    result->reset(new GfapiRandomAccessFile(fd, fname));
    return Status::OK();
  } else {
    result->reset(nullptr);
    return IOError(fname, errno);
  }
}

Status GlusterEnv::NewWritableFile(const std::string& fname,
                                   unique_ptr<WritableFile>* result,
                                   const EnvOptions& /**/) {
  if (GlusterEnv::FileExists(fname).ok()) {
    GlusterEnv::DeleteFile(fname);
  }

  glfs_fd_t* fd = glfs_creat(fs_, fname.c_str(), O_WRONLY | O_APPEND, 0644);

  if (fd) {
    result->reset(new GfapiWritableFile(fd, fname));
    return Status::OK();
  }

  result->reset(nullptr);
  return IOError(fname, errno);
}
// Create an object that represents a directory. Will fail if directory
// doesn't exist. If the directory exists, it will open the directory
// and create a new Directory object.
//
// On success, stores a pointer to the new Directory in
// *result and returns OK. On failure stores nullptr in *result and
// returns non-OK.
Status GlusterEnv::NewDirectory(const std::string& name,
                                unique_ptr<Directory>* result) {
  if (glfs_mkdir(fs_, name.c_str(), 0755) != 0) {
    return IOError(name, errno);
  }
  result->reset(new GfapiDir{glfs_opendir(fs_, name.c_str())});
  return Status::OK();
}

Status GlusterEnv::FileExists(const std::string& fname) {
  if (glfs_access(fs_, fname.c_str(), F_OK) == 0) {
    return Status::OK();
  }

  return IOError(fname, errno);
}

Status GlusterEnv::GetChildren(const std::string& dir,
                               std::vector<std::string>* result) {
  result->clear();

  unique_ptr<GfapiDir> dir_fd{new GfapiDir(glfs_opendir(fs_, dir.c_str()))};
  if (dir_fd == nullptr) {
    return IOError(dir, errno);
  }
  dirent entry{};
  dirent* res = nullptr;
  for (;;) {
    if (glfs_readdir_r(dir_fd->fd_, &entry, &res)) {
      return IOError("read dir entry fial", dir, errno);
    }

    if (res == nullptr) {
      break;
    }
    result->push_back(entry.d_name);
  }
  return Status::OK();
}

Status GlusterEnv::DeleteFile(const std::string& fname) {
  if (glfs_unlink(fs_, fname.c_str()) != 0) {
    return IOError(fname, errno);
  }

  return Status::OK();
}
// Create the specified directory. Returns error if directory exists.
Status GlusterEnv::CreateDir(const std::string& dirname) {
  if (glfs_mkdir(fs_, dirname.c_str(), 0755) != 0) {
    return IOError(dirname, errno);
  }

  return Status::OK();
}

// Creates directory if missing. Return Ok if it exists, or successful in
// Creating.
Status GlusterEnv::CreateDirIfMissing(const std::string& dirname) {
  const auto ret = glfs_mkdir(fs_, dirname.c_str(), 0755);
  if (ret == 0) {
    return Status::OK();
  }
  // if parent does not exisit
  switch (errno) {
    // The parent directory or an ancestor even higher does not exist
    case ENOENT:
      return Status::IOError(
          dirname,
          "The parent directory or an ancestor even higher does not exist");
    case ENOMEM:
      return IOError(dirname, ENOMEM);
    case EEXIST:
      return Status::OK();
    default:
      ROCKS_LOG_FATAL(mylog, "CreateDirIfMissing GlusterEnv call failed");
      return IOError(dirname, errno);
  }
}

Status GlusterEnv::DeleteDir(const std::string& dirname) {
  if (glfs_rmdir(fs_, dirname.c_str()) != 0) {
    return IOError(dirname, errno);
  }
  return Status::OK();
}
// NOTE: if we are sure that the file can only write in one cluster
// we can maintain a "file_size" member variable.
Status GlusterEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
  struct stat stbuf {};
  if (glfs_stat(fs_, fname.c_str(), &stbuf) != 0) {
    *file_size = 0;
    return IOError(fname, errno);
  } else {
    *file_size = stbuf.st_size;
  }
  return Status::OK();
}

Status GlusterEnv::GetFileModificationTime(const std::string& fname,
                                           uint64_t* file_mtime) {
  struct stat stbuf {};
  if (glfs_stat(fs_, fname.c_str(), &stbuf) != 0) {
    return IOError(fname, errno);
  }

  *file_mtime = stbuf.st_mtim.tv_sec;
  return Status::OK();
}

Status GlusterEnv::RenameFile(const std::string& src,
                              const std::string& target) {
  if (glfs_rename(fs_, src.c_str(), target.c_str())) {
    return IOError(src, errno);
  }
  return Status::OK();
}

Status GlusterEnv::LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;

  glfs_fd_t* fd;
  if (FileExists(fname).ok()) {
    fd = glfs_open(fs_, fname.c_str(), O_RDWR);
  } else {
    fd = glfs_creat(fs_, fname.c_str(), O_RDWR, 0644);
  }

  if (fd == nullptr) {
    return IOError(fname, errno);
  }

  if (!lockedFileTable.Insert(fname)) {
    glfs_close(fd);
    return Status::IOError("lock " + fname, "already held by process");
  }

  if (LockOrUnlock(fd, true) == -1) {
    glfs_close(fd);
    lockedFileTable.Erase(fname);
    return IOError("lock " + fname, errno);
  }

  *lock = new GfapiFlock(fd, fname);

  return Status::OK();
}

Status GlusterEnv::UnlockFile(FileLock* lock) {
  GfapiFlock* my_lock = static_cast<GfapiFlock*>(lock);
  if (lockedFileTable.Erase(my_lock->filename_) != 1) {
    errno = ENOLCK;
    return IOError(IOErrorMsg("unlock", my_lock->filename_), errno);
  }

  lockedFileTable.Erase(my_lock->filename_);
  glfs_close(my_lock->fd_);
  delete my_lock;
  return Status::OK();
}

void GlusterEnv::Schedule(void (*function)(void* arg1), void* arg, Priority pri,
                          void* tag, void (*unschedFunction)(void* arg)) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

void GlusterEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  ThreadPoolImpl::PthreadCall(
      "start thread", pthread_create(&t, nullptr, &StartThreadWrapper, state));
  ThreadPoolImpl::PthreadCall("lock", pthread_mutex_lock(&mu_));
  threads_to_join_.push_back(t);
  ThreadPoolImpl::PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

Status GlusterEnv::GetTestDirectory(std::string* path) {
  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *path = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
    *path = buf;
  }
  // Directory may already exist
  CreateDir(*path);
  return Status::OK();
}

Status GlusterEnv::NewLogger(const std::string& fname,
                             shared_ptr<Logger>* result) {
  *result = nullptr;
  ;
  const auto s = GlusterEnv::FileExists(fname);
  switch (s.code()) {
    case Status::kNotFound:
      break;
    case Status::kOk:
      GlusterEnv::DeleteFile(fname);
      break;
    default:
      return s;
  }

  // NOTE the log file can be open with O_APPEND
  glfs_fd_t* fd = glfs_creat(fs_, fname.c_str(), O_RDWR, 0644);
  GfapiWritableFile* f = new GfapiWritableFile(fd, fname);
  result->reset(new GfapiLogger(f, &GlusterEnv::gettid));
  return Status::OK();
}

uint64_t GlusterEnv::NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void GlusterEnv::SleepForMicroseconds(int micros) { usleep(micros); }

Status GlusterEnv::GetHostName(char* name, uint64_t len) {
  const int ret = gethostname(name, static_cast<size_t>(len));
  if (ret < 0) {
    if (errno == EFAULT || errno == EINVAL) {
      return Status::InvalidArgument(strerror(errno));
    }

    return IOError("GetHostName", name, errno);
  }
  return Status::OK();
}

Status GlusterEnv::GetCurrentTime(int64_t* unix_time) {
  const time_t ret = time(nullptr);
  if (ret == static_cast<time_t>(-1)) {
    return IOError("GetCurrentTime", "", errno);
  }
  *unix_time = static_cast<int64_t>(ret);
  return Status::OK();
}

Status GlusterEnv::GetAbsolutePath(const std::string& db_path,
                                   std::string* output_path) {
  if (!db_path.empty() && db_path[0] == '/') {
    *output_path = db_path;
    return Status::OK();
  }

  char the_path[4096] = {};
  char* ret = glfs_getcwd(fs_, the_path, 4096);
  if (ret == nullptr) {
    return IOError("[gluster] get GetAbsolutePath fail for ", db_path, errno);
  }

  *output_path = ret;
  return Status::OK();
}

void GlusterEnv::SetBackgroundThreads(int num, Priority pri) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  thread_pools_[pri].SetBackgroundThreads(num);
}

int GlusterEnv::GetBackgroundThreads(Priority pri) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  return thread_pools_[pri].GetBackgroundThreads();
}

void GlusterEnv::IncBackgroundThreadsIfNeeded(int num, Priority pri) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
}

std::string GlusterEnv::TimeToString(uint64_t time) {
  const auto seconds = static_cast<time_t>(time);
  struct tm t {};
  const int maxsize = 64;
  std::string dummy;
  dummy.reserve(maxsize);
  dummy.resize(maxsize);
  char* p = &dummy[0];
  localtime_r(&seconds, &t);
  snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
           t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
  return dummy;
}

Status GlusterEnv::Truncate(const std::string& filename, size_t size) {
  if (glfs_truncate(fs_, filename.c_str(), size)) {
    return IOError("[gluster] truncate", filename, errno);
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// GlusterEnv member GfapiRandomAccessFile
////////////////////////////////////////////////////////////////////////////////
namespace {

GfapiDir::~GfapiDir() {
  if (fd_) {
    glfs_closedir(fd_);
  }
}

// NOTE: check if the gluster would get eof?
Status GfapiSequentialFile::Read(size_t len, Slice* result, char* scratch) {
  size_t left = len;
  char* buf = scratch;
  while (left > 0) {
    const ssize_t done = glfs_read(fd_, buf, left, 0);
    if (done == 0) {
      break;
    }
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOError("[gluster] read file fail ", filename_, errno);
    }
    left += done;
    buf += done;
  }
  *result = Slice(scratch, len - left);
  assert(buf - scratch == static_cast<ssize_t>(len - left));
  return Status::OK();
}

Status GfapiSequentialFile::Skip(uint64_t n) {
  if (glfs_lseek(fd_, static_cast<off_t>(n), SEEK_CUR) < 0) {
    return IOError(filename_, errno);
  }

  return Status::OK();
}

GfapiSequentialFile::~GfapiSequentialFile() {
  if (fd_) {
    glfs_close(fd_);
  }
}

Status GfapiRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                   char* scratch) const {
  const auto bytes_read = glfs_pread(fd_, scratch, n, offset, 0);

  if (bytes_read < 0) {
    return IOError(filename_, errno);
  }
  *result = Slice(scratch, bytes_read);
  return Status::OK();
}

size_t GfapiRandomAccessFile::GetUniqueId(char* buf, size_t size) const {
  glfs_object* fd = glfs_h_lookupat(glfs_from_glfd(fd_), nullptr,
                                    filename_.c_str(), nullptr, 0);  // no fllow
  const auto len =
      glfs_h_extract_handle(fd, reinterpret_cast<unsigned char*>(buf), size);
  return len > 0 ? len : 0;
}

Status GfapiRandomAccessFile::FsRead(uint64_t offset, size_t len,
                                     void* buf) const {
  size_t left = len;
  auto ptr = static_cast<char*>(buf);
  while (left > 0) {
    const ssize_t done = glfs_pread(fd_, ptr, left, offset, 0);
    if (errno == EINTR) {
      continue;
    }
    if (done < 0) {
      return IOError("[gfapi] FsRead fail ", filename_, errno);
    }
    if (done == 0) {
      return IOError("[gfapi] FsRead fail, offset + len < file_size ",
                     filename_, EINVAL);
    }
    left -= done;
    ptr += done;
    offset += done;
  }
  return Status::OK();
}

GfapiWritableFile::GfapiWritableFile(glfs_fd_t* fd, const std::string& filename)
    : fd_(fd), filename_(filename), filesize_(0) {}

Status GfapiWritableFile::Append(const Slice& data) {
  const char* buf = data.data();
  auto left = data.size();
  while (left != 0) {
    const ssize_t done = glfs_write(fd_, buf, left, 0);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOError("while appeding to file", filename_, errno);
    }
    left -= done;
    buf += done;
  }
  filesize_ += data.size();
  return Status::OK();
}

Status GfapiWritableFile::Close() {
  if (fd_) {
    const int ret = glfs_close(fd_);
    fd_ = nullptr;
    if (ret < 0) {
      return IOError(filename_, errno);
    }
  }
  return Status::OK();
}

Status GfapiWritableFile::Flush() { return Status::OK(); }

Status GfapiWritableFile::Sync() {
  if (glfs_fdatasync(fd_) < 0) {
    return IOError(filename_, errno);
  }

  return Status::OK();
}

Status GfapiWritableFile::Fsync() {
  if (glfs_fsync(fd_) < 0) {
    return IOError(filename_, errno);
  }

  return Status::OK();
}

Status GfapiWritableFile::Truncate(uint64_t len) {
  const int ret = glfs_ftruncate(fd_, len);
  if (ret < 0) {
    return IOError(filename_, errno);
  }
  filesize_ = len;
  return Status::OK();
}

GfapiWritableFile::~GfapiWritableFile() {
  if (fd_) {
    glfs_close(fd_);
  }
}

GfapiLogger::GfapiLogger(GfapiWritableFile* f, uint64_t (*gettid)())
    : file_(f), gettid_(gettid) {
  ROCKS_LOG_DEBUG(mylog, "[gluster] GfapiLogger opened %s\n", file_->filename_);
}

}  // namespace

}  // namespace rocksdb
#endif