#!/bin/sh

#
# This script builds the cvmfs-release DEB for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the CernVM-FS release DEB package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2/cvmfs-release_1.0-0"

mkdir -p ${CVMFS_RESULT_LOCATION}

echo "copying the files to be packaged in place..."
mkdir -p $CVMFS_RESULT_LOCATION/DEBIAN
mkdir -p ${CVMFS_RESULT_LOCATION}/etc/apt/trusted.gpg.d
cp ${CVMFS_SOURCE_LOCATION}/packaging/debian/release/DEB-GPG-KEY-CernVM \
   ${CVMFS_RESULT_LOCATION}/etc/apt/trusted.gpg.d
cp ${CVMFS_SOURCE_LOCATION}/packaging/debian/release/control  \
   ${CVMFS_SOURCE_LOCATION}/packaging/debian/release/postinst \
   ${CVMFS_RESULT_LOCATION}/DEBIAN
cd ${CVMFS_RESULT_LOCATION}/..

echo "do the build..."
dpkg-deb --build ${CVMFS_RESULT_LOCATION}

# clean up the source tree
echo "cleaning up..."
rm -fR ${CVMFS_RESULT_LOCATION}
