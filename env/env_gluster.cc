#include <rocksdb/utilities/object_registry.h>
#include <sys/mman.h>
#include <util/string_util.h>
#ifdef USE_GLUSTER
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include "env/posix_logger.h"
#include "gluster/env_gluster.h"
#include "monitoring/thread_status_updater.h"
#include "options/db_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

extern "C" {
#include <glusterfs/api/glfs-handles.h>
#include <glusterfs/api/glfs.h>
}
namespace rocksdb {
namespace gfapi_internal {
////////////////////////////////////////////////////////////////////////////////
// helper
////////////////////////////////////////////////////////////////////////////////

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
    return static_cast<int>(lockedFiles.erase(filename));
  }
} lockedFileTable;

Status IOError(const std::string& context, int err_number) {
  return rocksdb::IOError(context, "", err_number);
}
using rocksdb::IOError;
struct AutoCleanableFd {
  int fd_ = -1;
  operator int&() { return fd_; }
  AutoCleanableFd& operator=(int fd) {
    fd_ = fd;
    return *this;
  }
  ~AutoCleanableFd() {
    if (fd_ > 0) {
      close(fd_);
    }
  }
};

int AccessMode(bool allow_others) { return allow_others ? 0644 : 0600; }
////////////////////////////////////////////////////////////////////////////////

Logger* mylog = nullptr;

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
class GfapiDir : public Directory {
 public:
  ~GfapiDir();
  GfapiDir(const GfapiDir&) = delete;
  GfapiDir& operator=(const GfapiDir&) = delete;
  GfapiDir(glfs_fd_t* fd) : fd_(fd), uuid("\0") {}
  Status Fsync() override { return Status::OK(); }
  size_t GetUniqueId(char* id, size_t size) const override;
  glfs_fd_t* fd_;
  char uuid[16];
};

class GfapiSequentialFile : public SequentialFile {
 public:
  GfapiSequentialFile(glfs_fd_t* fd, const std::string& filename)
      : fd_(fd), filename_(filename) {}
  Status Read(size_t len, Slice* result, char* scratch) override;
  Status Skip(uint64_t n) override;
  ~GfapiSequentialFile();

  Status InvalidateCache(size_t offset, size_t lenth) override;

 private:
  glfs_fd_t* fd_;
  const std::string filename_{};
};

class GfapiRandomAccessFile : public RandomAccessFile {
 private:
  const std::string filename_;
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
  // in "id"; otherwise, this function returns 0, in which case "id" may not
  // have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to each other by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  size_t GetUniqueId(char* buf, size_t size) const override;
#ifdef USE_FSREAD
  Status FsRead(uint64_t offset, size_t len, void* buf) const override;
#endif
 public:
  const std::string& GetFileName() const { return filename_; }
  Status InvalidateCache(size_t offset, size_t len) override;
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
  Status Truncate(uint64_t len) override;
  uint64_t GetFileSize() override { return filesize_; }
  ~GfapiWritableFile();
  GfapiWritableFile(const GfapiWritableFile&) = delete;

  Status InvalidateCache(size_t offset, size_t len) override;

 private:
  glfs_fd_t* fd_;
  std::string filename_{};
  uint64_t filesize_ = 0;
  friend class GfapiLogger;
};

class GfapiMmapFile : public GfapiRandomAccessFile {
 private:
  void* mmapped_region_;
  size_t length_;

 public:
  GfapiMmapFile(glfs_fd_t* fd, const std::string& filename, void* base,
                size_t length);
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override;
  void Hint(AccessPattern) override;
  ~GfapiMmapFile() override;
  Status InvalidateCache(size_t offset, size_t length) override;
};

class GfapiMemoryMappedFileBuffer : public MemoryMappedFileBuffer {
 public:
  GfapiMemoryMappedFileBuffer(void* _base, size_t _length)
      : MemoryMappedFileBuffer(_base, _length) {}
  virtual ~GfapiMemoryMappedFileBuffer() { munmap(this->base_, length_); };
};

// The object that implements the debug logs to reside in Glugser.
class GfapiLogger : public Logger {
 private:
  unique_ptr<GfapiWritableFile> file_{};
  uint64_t (*gettid_)();  // Return the thread id for the current thread
 protected:
  Status CloseHelper() {
    Status s = file_->Close();
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
    return s;
  }

 public:
  GfapiLogger(GfapiWritableFile* f, uint64_t (*gettid)());

  virtual ~GfapiLogger() {
    if (!closed_) {
      closed_ = true;
      CloseHelper();
    }
  }
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    const uint64_t thread_id = (*gettid_)();
    {
      std::string to_write;
      char* buf = nullptr;
      // the first part
      struct timeval now_tv {};
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t {};
      localtime_r(&seconds, &t);
      [[gnu::unused]] int ignored =
          asprintf(&buf, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                   t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                   t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                   static_cast<long long unsigned int>(thread_id));
      to_write.append(buf);

      // the second part
      ignored = vasprintf(&buf, format, ap);
      to_write.append(buf);

      free(buf);
      file_->Append(to_write);
      file_->Flush();
    }
  }
};
}  // namespace gfapi_internal
using namespace gfapi_internal;
////////////////////////////////////////////////////////////////////////////////
// GlusterEnv member functions
////////////////////////////////////////////////////////////////////////////////

GlusterEnv::GlusterEnv(const std::string& server, const std::string& vol,
                       const std::string& mount_point)
    : mount_point_(mount_point), default_env_(NewPosixEnv()) {
  fs_ = glfs_new(vol.c_str());
  glfs_set_volfile_server(fs_, "tcp", server.c_str(), 24007);
  auto ret = glfs_init(fs_);
  std::string tmp_file = "/tmp/" + std::to_string(getpid());

  if (ret) {
    fprintf(stderr,
            "Failed to connect GlusterFS server/volume: %s/%s, errno: %d\n",
            server.c_str(), vol.c_str(), errno);
    goto EXIT;
  }
  {
    // mkdir for tmp dir
    ret = mkdir((mount_point + "/tmp").c_str(), 0777);
    if (ret < 0 && errno != EEXIST) {
      fprintf(stderr, "Failed to create tmp dir, %s", strerror(errno));
      goto EXIT;
    }

    // test the mount_point points to gluster volume
    int fd = -1;
    do {
      fd = open((mount_point + tmp_file).c_str(), O_CREAT | O_RDWR | O_EXCL,
                0644);
      if (fd == EEXIST) {
        break;
      }
    } while (fd < 0 && errno == EINTR);
    if (fd > 0) {
      close(fd);
    }
    struct stat stbuf_mount {};
    struct stat stbuf_glfs {};
    ret = stat((mount_point + tmp_file).c_str(), &stbuf_mount);
    if (ret < 0) {
      fprintf(stderr, "Failed to get attr from %s, %s",
              (mount_point + tmp_file).c_str(), strerror(errno));
      goto EXIT;
    }
    ret = glfs_stat(fs_, tmp_file.c_str(), &stbuf_glfs);
    if (ret < 0) {
      fprintf(stderr, "Failed to get attr from gluster://%s, %s",
              tmp_file.c_str(), strerror(errno));
      goto EXIT;
    }
    if (stbuf_mount.st_ctim.tv_nsec != stbuf_glfs.st_ctim.tv_nsec ||
        stbuf_mount.st_ctim.tv_sec != stbuf_glfs.st_ctim.tv_sec ||
        stbuf_mount.st_size != stbuf_glfs.st_size)  // must check all ?
    {
      fprintf(stderr, "mount point is incorrect ");
      goto EXIT;
    }
    if (fd > 0) {
      unlink((mount_point + tmp_file).c_str());
    }
    return;
  }
EXIT:

  glfs_fini(fs_);
  exit(ret);
}

GlusterEnv::~GlusterEnv() {
  glfs_fini(fs_);
  fputs("Destroying GlusterEnv\n", stderr);
}

Status GlusterEnv::NewSequentialFile(const std::string& filename,
                                     unique_ptr<SequentialFile>* result,
                                     const EnvOptions& /**/) {
  *result = nullptr;
  glfs_fd_t* fd = glfs_open(fs_, filename.c_str(), O_RDONLY);
  if (fd) {
    result->reset(new GfapiSequentialFile(fd, filename));
    return Status::OK();
  }

  result->reset(nullptr);
  return IOError(filename, errno);
}

Status GlusterEnv::NewRandomAccessFile(const std::string& filename,
                                       unique_ptr<RandomAccessFile>* result,
                                       const EnvOptions& env) {
  glfs_fd_t* fd = glfs_open(fs_, filename.c_str(), O_RDONLY);

  if (fd) {
    if (!env.use_mmap_reads) {  // for non-direct and non-mmapped file
      result->reset(new GfapiRandomAccessFile(fd, filename));
      return Status::OK();
    }
    // NOTE direct_io will be ignored.
    AutoCleanableFd posix_fd;
    do {
      posix_fd = open(GenerateMountPath(filename).c_str(), O_RDONLY,
                      AccessMode(allow_non_owner_access_));
    } while (posix_fd && errno == EINTR);
    if (posix_fd < 0) {
      return IOError("while open a mmapped file for random read", filename,
                     errno);
    }
    uint64_t size;
    if (!GetFileSize(filename, &size).ok()) {
      return IOError("while mmap file for read when get file size", filename,
                     errno);
    }
    // use MAP_PRIVATE because of readonly
    void* base = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, posix_fd, 0);
    if (base == MAP_FAILED) {
      return IOError("while mmap file for read", filename, errno);
    }
    result->reset(new GfapiMmapFile(fd, filename, base, size));
    return Status::OK();
  }
  // open file with gfapi fail
  result->reset(nullptr);
  return IOError(filename, errno);
}

Status GlusterEnv::NewWritableFile(const std::string& filename,
                                   unique_ptr<WritableFile>* result,
                                   const EnvOptions& /**/) {
  if (GlusterEnv::FileExists(filename).ok()) {
    GlusterEnv::DeleteFile(filename);
  }

  glfs_fd_t* fd = glfs_creat(fs_, filename.c_str(), O_WRONLY | O_APPEND,
                             AccessMode(allow_non_owner_access_));

  if (fd) {
    result->reset(new GfapiWritableFile(fd, filename));
    return Status::OK();
  }

  result->reset(nullptr);
  switch (errno) {
    // The parent directory or an ancestor even higher does not exist
    case EACCES:
    case ENOENT:
    case ENOTDIR:
      return Status::NotFound();
    case ENOMEM:
      return IOError(filename, ENOMEM);
    default:
      return IOError(filename, errno);
  }
}

Status GlusterEnv::NewDirectory(const std::string& name,
                                unique_ptr<Directory>* result) {
  result->reset();
  struct stat stbuf {};
  const auto glfs_object =
      glfs_h_lookupat(fs_, nullptr, name.c_str(), &stbuf, 0);
  if (!glfs_object) {
    if (errno == ENOENT) {
      return Status::NotFound();
    }
    return IOError("[gfapi] NewDirectory fail: ", name, errno);
  }
  if (!S_ISDIR(stbuf.st_mode)) {
    std::string info =
        "[gfapi] while open directory" + name + " but isn't a dir";
    return Status::InvalidArgument(info);
  }
  const int uuid_size = 16;
  char buf[uuid_size] = {};
  const auto len =
      glfs_h_extract_handle(glfs_object, reinterpret_cast<unsigned char*>(buf),
                            static_cast<int>(uuid_size));
  const auto ptr = new GfapiDir{glfs_opendir(fs_, name.c_str())};
  memcpy(ptr->uuid, buf, len);
  result->reset(ptr);
  return Status::OK();
}

Status GlusterEnv::FileExists(const std::string& filename) {
  if (glfs_access(fs_, filename.c_str(), F_OK) == 0) {
    return Status::OK();
  }
  const int err = errno;
  switch (err) {
    case EACCES:
    case ELOOP:
    case ENAMETOOLONG:
    case ENOENT:
    case ENOTDIR:
      return Status::NotFound();
    default:
      assert(err == EIO || err == ENOMEM);
      return Status::IOError("Unexpected error(" + ToString(err) +
                             ") accessing file `" + filename + "' ");
  }
}

// get subfile name of dir. The "." and ".." will be held in result.
Status GlusterEnv::GetChildren(const std::string& dir,
                               std::vector<std::string>* result) {
  result->clear();
  const unique_ptr<GfapiDir> dir_fd(
      new GfapiDir(glfs_opendir(fs_, dir.c_str())));
  if (dir_fd->fd_ == nullptr) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound(dir);
      default:
        return IOError("[gfapi] while open dir", dir, errno);
    }
  }
  dirent entry{};
  dirent* res = nullptr;
  for (;;) {
    if (glfs_readdir_r(dir_fd->fd_, &entry, &res)) {
      return IOError("read dir entry fail", dir, errno);
    }

    if (res == nullptr) {
      break;
    }
    result->push_back(entry.d_name);
  }
  return Status::OK();
}

Status GlusterEnv::DeleteFile(const std::string& filename) {
  if (glfs_unlink(fs_, filename.c_str()) != 0) {
    return IOError(filename, errno);
  }

  return Status::OK();
}

Status GlusterEnv::CreateDir(const std::string& dirname) {
  if (glfs_mkdir(fs_, dirname.c_str(), 0755) != 0) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound(dirname);
      default:
        return IOError("dirname", dirname, errno);
    }
  }
  return Status::OK();
}

Status GlusterEnv::CreateDirIfMissing(const std::string& dirname) {
  const auto ret = glfs_mkdir(fs_, dirname.c_str(), 0755);
  if (ret == 0) {
    return Status::OK();
  }
  // if parent does not exisit
  switch (errno) {
    // The parent directory or an ancestor even higher does not exist
    case EACCES:
    case ENOENT:
    case ENOTDIR:
      return Status::NotFound();
    case ENOMEM:
      return IOError(dirname, ENOMEM);
    case EEXIST:
      return Status::OK();
    default:
      return IOError(dirname, errno);
  }
}

Status GlusterEnv::DeleteDir(const std::string& dirname) {
  if (glfs_rmdir(fs_, dirname.c_str()) != 0) {
    switch (errno) {
      case ENOENT:
        return Status::IOError();
      case ENODATA:
      case ENOTDIR:
        return Status::NotFound(Status::kNone);
      case EINVAL:
        return Status::InvalidArgument(Status::kNone);
      case EIO:
      default:
        return IOError(dirname, errno);
    }
  }
  return Status::OK();
}

// if we are sure that the file can only write in one cluster we can maintain a
// "file_size" member variable.
Status GlusterEnv::GetFileSize(const std::string& filename,
                               uint64_t* file_size) {
  struct stat stbuf {};
  if (glfs_stat(fs_, filename.c_str(), &stbuf) != 0) {
    *file_size = 0;
    return IOError(filename, errno);
  } else {
    *file_size = stbuf.st_size;
  }
  return Status::OK();
}

Status GlusterEnv::GetFileModificationTime(const std::string& filename,
                                           uint64_t* mtime) {
  struct stat stbuf {};
  if (glfs_stat(fs_, filename.c_str(), &stbuf) != 0) {
    return IOError(filename, errno);
  }

  *mtime = stbuf.st_mtim.tv_sec;
  return Status::OK();
}

Status GlusterEnv::RenameFile(const std::string& src,
                              const std::string& target) {
  if (glfs_rename(fs_, src.c_str(), target.c_str())) {
    return IOError("[gluster] rename fail: ", src, errno);
  }
  return Status::OK();
}

// Lock file. create if missing.
//
// It seems that LockFile is used for preventing other instance of RocksDB
// from opening up the database at the same time. From RocksDB source code,
// the invokes of LockFile are at following locations:
//
//  ./db/db_impl.cc:1159:
//  s = env_->LockFile(LockFileName(dbname_),&db_lock_); // DBImpl::Recover
//  ./db/db_impl.cc:5839:
//  Status result = env->LockFile(lockname, &lock); //Status DestroyDB
//
// When db recovery and db destroy, RocksDB will call LockFile
Status GlusterEnv::LockFile(const std::string& filename, FileLock** lock) {
  *lock = nullptr;

  glfs_fd_t* fd;
  if (FileExists(filename).ok()) {
    fd = glfs_open(fs_, filename.c_str(), O_RDWR);
  } else {
    fd = glfs_creat(fs_, filename.c_str(), O_RDWR,
                    AccessMode(allow_non_owner_access_));
  }

  if (fd == nullptr) {
    return IOError(filename, errno);
  }

  if (!lockedFileTable.Insert(filename)) {
    glfs_close(fd);
    return Status::IOError("lock " + filename, "already held by process");
  }

  if (LockOrUnlock(fd, true) == -1) {
    glfs_close(fd);
    lockedFileTable.Erase(filename);
    return IOError("lock " + filename, errno);
  }

  *lock = new GfapiFlock(fd, filename);

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

Status GlusterEnv::NewLogger(const std::string& filename,
                             shared_ptr<Logger>* result) {
  *result = nullptr;
  const auto s = GlusterEnv::FileExists(filename);
  switch (s.code()) {
    case Status::kNotFound:
      break;
    case Status::kOk:
      GlusterEnv::DeleteFile(filename);
      break;
    default:
      return s;
  }

  // NOTE the log file can be open with O_APPEND
  glfs_fd_t* fd = glfs_creat(fs_, filename.c_str(), O_RDWR,
                             AccessMode(allow_non_owner_access_));
  if (!fd) {
    return IOError("[gluster] create ", filename, errno);
  }

  auto wfile = new GfapiWritableFile(fd, filename);

  std::unique_ptr<GfapiLogger> logger{
      new GfapiLogger(wfile, &GlusterEnv::gettid)};

  result->reset(logger.release());
  return Status::OK();
}

// Get full directory name for this db through mount point
Status GlusterEnv::GetAbsolutePath(const std::string& db_path,
                                   std::string* output_path) {
  *output_path = db_path;
  return Status::OK();
}

Status GlusterEnv::Truncate(const std::string& filename, size_t size) {
  if (glfs_truncate(fs_, filename.c_str(), size)) {
    return IOError("[gluster] truncate", filename, errno);
  }
  return Status::OK();
}

// Opens `filename` as a memory-mapped file.
Status GlusterEnv::NewMemoryMappedFileBuffer(
    const std::string& filename, unique_ptr<MemoryMappedFileBuffer>* result) {
  AutoCleanableFd posix_fd;
  while (posix_fd < 0) {
    posix_fd.fd_ = open(GenerateMountPath(filename).c_str(), O_RDWR, 0644);

    if (posix_fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOError("While open file for raw mmap buffer access", filename,
                     errno);
    }
  }
  uint64_t size = 0;
  const auto status = GetFileSize(filename, &size);
  if (!status.ok()) {
    return status;
  }

  const auto base = mmap(nullptr, static_cast<size_t>(size),
                         PROT_READ | PROT_WRITE, MAP_SHARED, posix_fd, 0);
  if (base == MAP_FAILED) {
    return IOError("while mmap file for read", filename, errno);
  }

  result->reset(
      new GfapiMemoryMappedFileBuffer(base, static_cast<size_t>(size)));
  return status;
}

// Get full path through mount path.
std::string GlusterEnv::GenerateMountPath(const std::string& filename) const {
  return this->mount_point_ + "/" + filename;
}

void GlusterEnv::Schedule(void (*function)(void* arg), void* arg, Priority pri,
                          void* tag, void (*unschedFunction)(void* arg)) {
  return default_env_->Schedule(function, arg, pri, tag, unschedFunction);
}

void GlusterEnv::StartThread(void (*function)(void* arg), void* arg) {
  return default_env_->StartThread(function, arg);
}

void GlusterEnv::SetBackgroundThreads(int number, Priority pri) {
  return default_env_->SetBackgroundThreads(number, pri);
}

int GlusterEnv::GetBackgroundThreads(Priority pri) {
  return default_env_->GetBackgroundThreads(pri);
}

void GlusterEnv::IncBackgroundThreadsIfNeeded(int number, Priority pri) {
  return default_env_->IncBackgroundThreadsIfNeeded(number, pri);
}

std::string GlusterEnv::TimeToString(uint64_t time) {
  return default_env_->TimeToString(time);
}

uint64_t GlusterEnv::NowMicros() { return default_env_->NowMicros(); }

void GlusterEnv::SleepForMicroseconds(int micros) {
  default_env_->SleepForMicroseconds(micros);
}

Status GlusterEnv::GetHostName(char* name, uint64_t len) {
  return default_env_->GetHostName(name, len);
}

Status GlusterEnv::GetCurrentTime(int64_t* unix_time) {
  return default_env_->GetCurrentTime(unix_time);
}

Status GlusterEnv::ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     unique_ptr<WritableFile>* result,
                                     const EnvOptions&) {
  result->reset(nullptr);
  Status s = RenameFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  auto fd = glfs_open(fs_, fname.c_str(), O_WRONLY | O_APPEND);
  if (!fd) {
    switch (errno) {
      // The parent directory or an ancestor even higher does not exist
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound();
      case ENOMEM:
        return IOError(fname, ENOMEM);
      default:
        return IOError(fname, errno);
    }
  }

  result->reset(new GfapiWritableFile(fd, fname));
  return Status::OK();
}

Status GlusterEnv::AreFilesSame(const std::string& first,
                                const std::string& second, bool* res) {
  struct stat statbuf[2];
  std::string msg("stat file");
  ;
  if (glfs_stat(fs_, first.c_str(), &statbuf[0]) != 0) {
    msg.append(first);
  } else if (glfs_stat(fs_, second.c_str(), &statbuf[1]) != 0) {
    msg.append(second);
  } else {  // files ok.
    if (major(statbuf[0].st_dev) != major(statbuf[1].st_dev) ||
        minor(statbuf[0].st_dev) != minor(statbuf[1].st_dev) ||
        statbuf[0].st_ino != statbuf[1].st_ino) {
      *res = false;
    } else {
      *res = true;
    }
    return Status::OK();
  }

  switch (errno) {
    // The parent directory or an ancestor even higher does not exist
    case EACCES:
    case ENOENT:
    case ENOTDIR:
      return Status::NotFound(msg);
    case ENOMEM:
      return IOError(msg, ENOMEM);
    default:
      return IOError(msg, errno);
  }
}

Status GlusterEnv::ReopenWritableFile(const std::string& fname,
                                      unique_ptr<WritableFile>* result,
                                      const EnvOptions&) {
  const auto fd = glfs_open(fs_, fname.c_str(), O_WRONLY | O_APPEND);
  if (!fd) {
    result->reset(nullptr);
    switch (errno) {
      // The parent directory or an ancestor even higher does not exist
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound();
      case ENOMEM:
        return IOError(fname, ENOMEM);
      default:
        return IOError(fname, errno);
    }
  }
  result->reset(new GfapiWritableFile(fd, fname));
  return Status::OK();
}

// Generate a NewRandomRWFile. there is no need to support it.
Status GlusterEnv::NewRandomRWFile(const std::string&,
                                   unique_ptr<RandomRWFile>*,
                                   const EnvOptions&) {
  return Status::NotSupported();
}

Status GlusterEnv::LinkFile(const std::string& src, const std::string& target) {
  Status result;
  if (glfs_link(fs_, src.c_str(), target.c_str()) != 0) {
    if (errno == EXDEV) {
      return Status::NotSupported("No cross FS links allowed");
    }
    result = IOError("while link file to " + target, src, errno);
  }
  return result;
}

Status GlusterEnv::NumFileLinks(const std::string& fname, uint64_t* count) {
  struct stat s {};
  if (glfs_stat(fs_, fname.c_str(), &s) != 0) {
    return IOError("while stat a file for num file links", fname, errno);
  }
  *count = static_cast<uint64_t>(s.st_nlink);
  return Status::OK();
}

Status GlusterEnv::GetFreeSpace(const std::string& path, uint64_t* size) {
  struct statvfs buf {};
  if (glfs_statvfs(fs_, path.c_str(), &buf) < 0) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound();
      default:
        return IOError("while doing glfs_statvfs", path, errno);
    }
  }
  *size = buf.f_bsize * buf.f_bfree;
  return Status::OK();
}

Status GlusterEnv::SetAllowNonOwnerAccess(bool allow_non_owner_access) {
  allow_non_owner_access_ = allow_non_owner_access;
  return Status::OK();
}

// Get all subfiles' name and attributes of dir. The "." and ".." will be held
// in result.
Status GlusterEnv::GetChildrenFileAttributes(
    const std::string& dir, std::vector<FileAttributes>* result) {
  result->clear();
  const auto fd = glfs_opendir(fs_, dir.c_str());
  if (!fd) {
    switch (errno) {
      case EACCES:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound();
      default:
        return IOError("[gfapi] GetChildrenFileAttributes fail", dir, errno);
    }
  }
  struct stat stbuf {};

  for (auto entry = glfs_readdirplus(fd, &stbuf); entry != nullptr;
       entry = glfs_readdirplus(fd, &stbuf)) {
    result->push_back(
        FileAttributes{entry->d_name, static_cast<uint64_t>(stbuf.st_size)});
  }
  glfs_closedir(fd);
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// GlusterEnv member GfapiRandomAccessFile
////////////////////////////////////////////////////////////////////////////////
namespace gfapi_internal {

GfapiDir::~GfapiDir() {
  if (fd_) {
    glfs_closedir(fd_);
  }
}

size_t GfapiDir::GetUniqueId(char* id, size_t size) const {
  const auto len = std::min(sizeof(uuid), size);
  memcpy(id, uuid, len);
  return len;
}

// Read up to "len" bytes from the file.
// Now, glfs_read may reads over than file size on dispersed volume
// For details, see https://github.com/gluster/glusterfs/issues/394
//
// REQUIRES: External synchronization
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
    left -= done;
    buf += done;
  }
  *result = Slice(scratch, len - left);
  assert(buf - scratch == static_cast<ssize_t>(len - left));
  return Status::OK();
}

// Skip "n" bytes from the file. This is guaranteed to be no
// slower that reading the same data, but may be faster.
//
// If end of file is reached, skipping will stop at the end of the
// file, and Skip will return OK.
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

Status GfapiSequentialFile::InvalidateCache(size_t, size_t) {
  return Status::OK();
}

// Similar to pread on normal filesystem.
// Now, glfs_pread may reads over than file size on dispersed volume
// For details, see https://github.com/gluster/glusterfs/issues/394
Status GfapiRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                   char* scratch) const {
  const auto bytes_read = glfs_pread(fd_, scratch, n, offset, 0);

  if (bytes_read < 0) {
    return IOError(filename_, errno);
  }
  *result = Slice(scratch, bytes_read);
  return Status::OK();
}

// Get unique id for each file and guarantee this id is different for each file.
size_t GfapiRandomAccessFile::GetUniqueId(char* buf, size_t size) const {
  glfs_object* fd =
      glfs_h_lookupat(glfs_from_glfd(fd_), nullptr, filename_.c_str(), nullptr,
                      0);  // 0 means no fllow
  const auto len = glfs_h_extract_handle(
      fd, reinterpret_cast<unsigned char*>(buf), static_cast<int>(size));
  return len > 0 ? len : 0;
}
#ifdef USE_FSREAD

// Read file through libgfapi.
//
// Similar to read in GfapiRandomAccessFile::Read, but error will occured when
// offset + len < filesize, which means the buf must be full filled when returns
// ok.
//
// IOError(EINVAL) for offset + len < file_size
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
#endif  // USE_FSREAD

Status GfapiRandomAccessFile::InvalidateCache(size_t, size_t) {
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

Status GfapiWritableFile::InvalidateCache(size_t, size_t) {
  return Status::OK();
}

// Read up to "n" bytes from the file. The offset + n > length_ must
// be ensured.
Status GfapiMmapFile::Read(uint64_t offset, size_t n, Slice* result,
                           char* /*scratch*/) const {
  Status s;
  if (offset > length_) {
    *result = Slice();
    return IOError("While mmap read offset " + ToString(offset) +
                       " larger than file length " + ToString(length_),
                   GetFileName(), EINVAL);
  }
  if (offset + n > length_) {
    n = static_cast<size_t>(length_ - offset);
  }
  *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
  return s;
}

// Modify access mode in mmapped file
void GfapiMmapFile::Hint(AccessPattern pattern) {
  if (use_direct_io()) {
    return;
  }
  switch (pattern) {
    case NORMAL:
      posix_madvise(mmapped_region_, 0, POSIX_FADV_NORMAL);
      break;
    case RANDOM:
      posix_madvise(mmapped_region_, 0, POSIX_FADV_RANDOM);
      break;
    case SEQUENTIAL:
      posix_madvise(mmapped_region_, 0, POSIX_FADV_SEQUENTIAL);
      break;
    case WILLNEED:
      posix_madvise(mmapped_region_, 0, POSIX_FADV_WILLNEED);
      break;
    case DONTNEED:
      posix_madvise(mmapped_region_, 0, POSIX_FADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}
// destructor, release the memory
GfapiMmapFile::~GfapiMmapFile() {
  if (munmap(mmapped_region_, length_) != 0) {
    fprintf(stdout, "failed to munmap %p length %" ROCKSDB_PRIszt " \n",
            mmapped_region_, length_);
  }
}

// Remove any kind of caching of data from the offset to offset + length
// of this file.
Status GfapiMmapFile::InvalidateCache(size_t offset, size_t /*length*/) {
  // free OS pages
  if (posix_madvise(static_cast<char*>(this->mmapped_region_) + offset, length_,
                    POSIX_FADV_DONTNEED) == 0) {
    return Status::OK();
  }
  return IOError("While fadvise NotNeeded mmapped file", GetFileName(), errno);
}

GfapiMmapFile::GfapiMmapFile(glfs_fd_t* fd, const std::string& filename,
                             void* base, size_t length)
    : GfapiRandomAccessFile(fd, filename),
      mmapped_region_(base),
      length_(length) {}

// interface for writing log messages
GfapiLogger::GfapiLogger(GfapiWritableFile* f, uint64_t (*gettid)())
    : file_(f), gettid_(gettid) {}

}  // namespace gfapi_internal

int GlusterEnv::UnSchedule(void* arg, Priority pri) {
  return default_env_->UnSchedule(arg, pri);
}

void GlusterEnv::WaitForJoin() { default_env_->WaitForJoin(); }

unsigned GlusterEnv::GetThreadPoolQueueLen(Priority pri) const {
  return default_env_->GetThreadPoolQueueLen(pri);
}

uint64_t GlusterEnv::NowNanos() { return default_env_->NowNanos(); }

EnvOptions GlusterEnv::OptimizeForLogRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions GlusterEnv::OptimizeForManifestRead(
    const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions GlusterEnv::OptimizeForLogWrite(const EnvOptions& env_options,
                                           const DBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_env_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_env_options;
}

EnvOptions GlusterEnv::OptimizeForManifestWrite(
    const EnvOptions& env_options) const {
  return env_options;
}

EnvOptions GlusterEnv::OptimizeForCompactionTableWrite(
    const EnvOptions& env_options,
    const ImmutableDBOptions& /*immutable_ops*/) const {
  return env_options;
}

EnvOptions GlusterEnv::OptimizeForCompactionTableRead(
    const EnvOptions& env_options,
    const ImmutableDBOptions& /*db_options*/) const {
  return env_options;
}

void GlusterEnv::LowerThreadPoolIOPriority(Priority pri) {
  default_env_->LowerThreadPoolIOPriority(pri);
}

void GlusterEnv::LowerThreadPoolCPUPriority(Priority pri) {
  default_env_->LowerThreadPoolCPUPriority(pri);
}

std::string GlusterEnv::GenerateUniqueId() {
  return default_env_->GenerateUniqueId();
}

Status GlusterEnv::GetThreadList(std::vector<ThreadStatus>* thread_list) {
  return default_env_->GetThreadList(thread_list);
}

uint64_t GlusterEnv::GetThreadID() const { return default_env_->GetThreadID(); }

ThreadStatusUpdater* GlusterEnv::GetThreadStatusUpdater() const {
  return default_env_->GetThreadStatusUpdater();
}

/////////////////////////////////////////////////////////////////////////////////////////////

static Registrar<Env> reg_gluster{
    R"(gfapi://(.+)(\s+)(.+)(\s+)(.+))",
    [](const std::string& url, std::unique_ptr<Env>* env_ptr) {
      using namespace std;
      const regex pattern(R"(gfapi://(.+)\s+(.+)\s+(.+))");
      smatch pieces;
      regex_match(url, pieces, pattern);
      assert(pieces.size() == 4);
      const string host = pieces[1];
      const string vol = pieces[2];
      const string mount_point = pieces[3];
      env_ptr->reset(new GlusterEnv(host, vol, mount_point));
      return env_ptr->get();
    }};
}  // namespace rocksdb
#endif