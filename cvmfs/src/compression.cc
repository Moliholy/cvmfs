/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Allmost all of the functions return true on success, otherwise false.
 *
 * TODO: think about code deduplication
 */

#define _FILE_OFFSET_BITS 64

#include "compression.h"

#include <stdlib.h>
#include <sys/stat.h>

#include <cstring>

#include "logging.h"
#include "hash.h"
extern "C" {
#include "sha1.h"
#include "smalloc.h"
}


using namespace std;  // NOLINT


static bool CopyFile2File(FILE *fsrc, FILE *fdest) {
  unsigned char buf[1024];
  rewind(fsrc);
  rewind(fdest);

  size_t have;
  do {
    have = fread(buf, 1, 1024, fsrc);
    if (fwrite(buf, 1, have, fdest) != have)
      return false;
  } while (have == 1024);
  return true;
}


bool CopyPath2Path(const string &src, const string &dest) {
  FILE *fsrc = NULL;
  FILE *fdest = NULL;
  int retval = -1;
  struct stat info;

  fsrc = fopen(src.c_str(), "r");
  if (!fsrc) goto file_copy_final;

  fdest = fopen(dest.c_str(), "w");
  if (!fdest) goto file_copy_final;

  if (!CopyFile2File(fsrc, fdest)) goto file_copy_final;
  retval = fstat(fileno(fsrc), &info);
  retval |= fchmod(fileno(fdest), info.st_mode);

 file_copy_final:
  if (fsrc) fclose(fsrc);
  if (fdest) fclose(fdest);
  return retval == 0;
}


namespace zlib {

const unsigned kZChunk = 16384;
const unsigned kBufferSize = 32768;

bool CompressInit(z_stream *strm) {
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->next_in = Z_NULL;
  strm->avail_in = 0;
  return deflateInit(strm, Z_DEFAULT_COMPRESSION) == 0;
}


bool DecompressInit(z_stream *strm) {
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->avail_in = 0;
  strm->next_in = Z_NULL;
  return inflateInit(strm) == 0;
}


void CompressFini(z_stream *strm) {
  (void)deflateEnd(strm);
}


void DecompressFini(z_stream *strm) {
  (void)inflateEnd(strm);
}


StreamStates DecompressZStream2File(z_stream *strm, FILE *f, const void *buf,
                                    const int64_t size)
{
  unsigned char out[kZChunk];
  int z_ret;
  int64_t pos = 0;

  do {
    strm->avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    strm->next_in = ((unsigned char *)buf)+pos;

    // Run inflate() on input until output buffer not full
    do {
      strm->avail_out = kZChunk;
      strm->next_out = out;
      z_ret = inflate(strm, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
          return kStreamError;
      }
      size_t have = kZChunk - strm->avail_out;
      if (fwrite(out, 1, have, f) != have || ferror(f))
        return kStreamError;
    } while (strm->avail_out == 0);

    pos += kZChunk;
  } while (pos < size);

  return (z_ret == Z_STREAM_END ? kStreamEnd : kStreamContinue);
}


bool CompressPath2Path(const string &src, const string &dest) {
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc) {
    LogCvmfs(kLogCompress, kLogDebug,  "open %s as compression source failed",
             src.c_str());
    return false;
  }

  FILE *fdest = fopen(dest.c_str(), "w");
  if (!fdest) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression destination "
             "failed", dest.c_str());
    fclose(fsrc);
    return false;
  }

  LogCvmfs(kLogCompress, kLogDebug, "opened %s and %s for compression",
           src.c_str(), dest.c_str());
  const bool result = CompressFile2File(fsrc, fdest);

  fclose(fsrc);
  fclose(fdest);
  return result;
}


bool CompressPath2Path(const string &src, const string &dest,
                       hash::t_sha1 *compressed_hash)
{
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression source failed",
             src.c_str());
    return false;
  }

  FILE *fdest = fopen(dest.c_str(), "w");
  if (!fdest) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression destination "
             "failed", dest.c_str());
    fclose(fsrc);
    return false;
  }

  LogCvmfs(kLogCompress, kLogDebug, "opened %s and %s for compression",
           src.c_str(), dest.c_str());
  bool result = false;
  if (!CompressFile2File(fsrc, fdest, compressed_hash))
    goto compress_path2path_final;
  struct stat info;
  if (fstat(fileno(fsrc), &info) != 0) goto compress_path2path_final;
  // TODO(jakob): open in the right mode from the beginning
  if (fchmod(fileno(fdest), info.st_mode) != 0) goto compress_path2path_final;

  result = true;

 compress_path2path_final:
  fclose(fsrc);
  fclose(fdest);
  return result;
}


bool DecompressPath2Path(const string &src, const string &dest) {
  FILE *fsrc = NULL;
  FILE *fdest = NULL;
  int result = false;

  fsrc = fopen(src.c_str(), "r");
  if (!fsrc) goto decompress_path2path_final;

  fdest = fopen(dest.c_str(), "w");
  if (!fdest) goto decompress_path2path_final;

  result = DecompressFile2File(fsrc, fdest);

 decompress_path2path_final:
  if (fsrc) fclose(fsrc);
  if (fdest) fclose(fdest);
  return result;
}


bool CompressFile2Null(FILE *fsrc, hash::t_sha1 *compressed_hash) {
  int z_ret, flush;
  bool result = -1;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];
  sha1_context_t sha1_ctx;

  if (!CompressInit(&strm)) goto compress_file2null_final;
  sha1_init(&sha1_ctx);

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2null_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2null_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      sha1_update(&sha1_ctx, out, have);
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2null_final;

  sha1_final(compressed_hash->digest, &sha1_ctx);
  result = true;

  // Clean up and return
 compress_file2null_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}


bool CompressFile2File(FILE *fsrc, FILE *fdest) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];

  if (!CompressInit(&strm)) goto compress_file2file_final;

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2file_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2file_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
        goto compress_file2file_final;
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2file_final;

  result = true;

  // Clean up and return
 compress_file2file_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}


bool CompressFile2File(FILE *fsrc, FILE *fdest, hash::t_sha1 *compressed_hash) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];
  sha1_context_t sha1_ctx;

  if (!CompressInit(&strm)) goto compress_file2file_hashed_final;
  sha1_init(&sha1_ctx);

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2file_hashed_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2file_hashed_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
        goto compress_file2file_hashed_final;
      sha1_update(&sha1_ctx, out, have);
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // Stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2file_hashed_final;

  sha1_final(compressed_hash->digest, &sha1_ctx);
  result = true;

  // Clean up and return
 compress_file2file_hashed_final:
  DecompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}


bool DecompressFile2File(FILE *fsrc, FILE *fdest) {
  bool result = false;
  StreamStates stream_state;
  z_stream strm;
  size_t have;
  unsigned char buf[kBufferSize];

  if (!DecompressInit(&strm)) goto decompress_file2file_final;

  while ((have = fread(buf, 1, kBufferSize, fsrc)) > 0) {
    stream_state = DecompressZStream2File(&strm, fdest, buf, have);
    if (stream_state == kStreamError)
      goto decompress_file2file_final;
  }
  LogCvmfs(kLogCompress, kLogDebug, "end of decompression, state=%d, error=%d",
           stream_state, ferror(fsrc));
  if ((stream_state != kStreamEnd) || ferror(fsrc))
    goto decompress_file2file_final;

  result = true;

 decompress_file2file_final:
  DecompressFini(&strm);
  return result;
}


/**
 * User of this function has to free out_buf.
 */
bool CompressMem2Mem(const void *buf, const int64_t size,
                    void **out_buf, int64_t *out_size)
{
  unsigned char out[kZChunk];
  int z_ret;
  int flush;
  z_stream strm;
  int64_t pos = 0;
  uint64_t alloc_size = kZChunk;

  if (!CompressInit(&strm)) return false;

  *out_buf = smalloc(alloc_size);
  *out_size = 0;

  do {
    strm.avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    flush = (pos + kZChunk) >= size ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = ((unsigned char *)buf) + pos;

    // Run deflate() on input until output buffer not full
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);
      if (z_ret == Z_STREAM_ERROR) {
        CompressFini(&strm);
        free(*out_buf);
        *out_buf = NULL;
        *out_size = 0;
        return false;
      }
      size_t have = kZChunk - strm.avail_out;
      if (*out_size+have > alloc_size) {
        alloc_size *= 2;
        *out_buf = srealloc(*out_buf, alloc_size);
      }
      memcpy(static_cast<unsigned char *>(*out_buf) + *out_size, out, have);
      *out_size += have;
    } while (strm.avail_out == 0);

    pos += kZChunk;
  } while (flush != Z_FINISH);

  CompressFini(&strm);
  if (z_ret != Z_STREAM_END) {
    free(*out_buf);
    *out_buf = NULL;
    *out_size = 0;
    return false;
  } else {
    return true;
  }
}


/**
 * User of this function has to free out_buf.
 */
bool DecompressMem2Mem(const void *buf, const int64_t size,
                       void **out_buf, int64_t *out_size)
{
  unsigned char out[kZChunk];
  int z_ret;
  z_stream strm;
  int64_t pos = 0;
  uint64_t alloc_size = kZChunk;

  if (!DecompressInit(&strm)) {
    *out_buf = NULL;
    *out_size = 0;
    return false;
  }

  *out_buf = smalloc(alloc_size);
  *out_size = 0;

  do {
    strm.avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    strm.next_in = ((unsigned char *)buf)+pos;

    // Run inflate() on input until output buffer not full
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = inflate(&strm, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
          DecompressFini(&strm);
          free(*out_buf);
          *out_buf = NULL;
          *out_size = 0;
          return false;
      }
      size_t have = kZChunk - strm.avail_out;
      if (*out_size+have > alloc_size) {
        alloc_size *= 2;
        *out_buf = srealloc(*out_buf, alloc_size);
      }
      memcpy(static_cast<unsigned char *>(*out_buf) + *out_size, out, have);
      *out_size += have;
    } while (strm.avail_out == 0);

    pos += kZChunk;
  } while (pos < size);

  DecompressFini(&strm);
  if (z_ret != Z_STREAM_END) {
    free(*out_buf);
    *out_buf = NULL;
    *out_size = 0;
    return false;
  } else {
    return 0;
  }
}

}  // namespace zlib
