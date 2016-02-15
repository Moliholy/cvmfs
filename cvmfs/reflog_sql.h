/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_SQL_H_
#define CVMFS_REFLOG_SQL_H_

#include "sql.h"

class ReflogDatabase : public sqlite::Database<ReflogDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static const std::string kFqrnKey;

  bool CreateEmptyDatabase();

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const { return true; }  // no implementation specific
                                                 // database compaction.

  bool InsertInitialValues(const std::string &repo_name);

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<ReflogDatabase>;
  ReflogDatabase(const std::string  &filename,
                 const OpenMode      open_mode) :
    sqlite::Database<ReflogDatabase>(filename, open_mode) {}
};


//------------------------------------------------------------------------------


class SqlReflog : public sqlite::Sql {
 public:
  enum ReferenceType {
    kRefCatalog,
    kRefCertificate,
    kRefHistory,
    kRefMetainfo
  };

 protected:
  std::string db_fields(const ReflogDatabase *database) const;
};


class SqlInsertReference : public SqlReflog {
 public:
  explicit SqlInsertReference(const ReflogDatabase *database);
  bool BindReference(const shash::Any    &reference_hash,
                     const ReferenceType  type);
};


class SqlCountReferences : public SqlReflog {
 public:
  explicit SqlCountReferences(const ReflogDatabase *database);
  uint64_t RetrieveCount();
};

#endif /* CVMFS_REFLOG_SQL_H_ */
