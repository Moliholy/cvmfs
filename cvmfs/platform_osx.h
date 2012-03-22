/**
 * This file is part of the CernVM File System.
 *
 * Mac OS X specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_OSX_H_
#define CVMFS_PLATFORM_OSX_H_

/**
 * UNIX domain sockets:
 * MSG_NOSIGNAL prevents send() from sending SIGPIPE
 * and EPIPE is return instead, where supported.
 * MSG_NOSIGNAL is Linux specific, SO_NOSIGPIPE is the Mac OS X equivalent.
 */
#define MSG_NOSIGNAL SO_NOSIGPIPE


/**
 * Spinlocks on OS X are not in pthread but in OS X specific APIs.
 */

#include <libkern/OSAtomic.h>

typedef OSSpinLock platform_spinlock;

inline int platform_spinlock_init(platform_spinlock *lock, int pshared) {
  *lock = 0;
  return 0;
}

inline int platform_spinlock_destroy(platform_spinlock *lock) { return 0; }

inline int platform_spinlock_trylock(platform_spinlock *lock) {
  return (OSSpinLockTry(lock)) ? 0 : -1;
}


/**
 * File system functions, Mac OS X has 64bit functions by default.
 */
#include <dirent.h>
#include <sys/stat.h>

typedef struct dirent platform_dirent64;

inline platform_dirent64 *platform_readdir(DIR *dirp) { return readdir(dirp); }

typedef struct stat platform_stat64;

inline int platform_stat(const char *path, platform_stat64 *buf) {
  return stat(path, buf);
}

inline int platform_lstat(const char *path, platform_stat64 *buf) {
  return lstat(path, buf);
}

inline int platform_fstat(int filedes, platform_stat64 *buf) {
  return fstat(filedes, buf);
}

/**
 * strdupa does not exist on OSX
 */
#include <alloca.h>
#include <cstring>
#define strdupa(s) strcpy(reinterpret_cast<char *> \
  (alloca(strlen((s)) + 1)), (s))

#endif  // CVMFS_PLATFORM_OSX_H_
