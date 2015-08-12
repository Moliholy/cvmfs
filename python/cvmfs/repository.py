#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import abc
import os
import glob
import urlparse
import tempfile
import requests
import collections
from datetime import datetime
import dateutil.parser
from dateutil.tz import tzutc
import zlib

import _common
import cvmfs
from manifest import Manifest
from catalog import Catalog
from history import History
from whitelist import Whitelist
from certificate import Certificate

class RepositoryNotFound(Exception):
    def __init__(self, repo_path):
        self.path = repo_path

    def __str__(self):
        return self.path + " not found"

class UnknownRepositoryType(Exception):
    def __init__(self, repo_fqrn, repo_type):
        self.fqrn = repo_fqrn
        self.type = repo_type

    def __str__(self):
        return self.fqrn + " (" + self.type + ")"

class ConfigurationNotFound(Exception):
    def __init__(self, repo, config_field):
        self.repo         = repo
        self.config_field = config_field

    def __str__(self):
        return repr(self.repo) + " " + self.config_field

class FileNotFoundInRepository(Exception):
    def __init__(self, repo, file_name):
        self.repo      = repo
        self.file_name = file_name

    def __str__(self):
        return repr(self.file_name)

class HistoryNotFound(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)

class CannotReplicate(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)

class NestedCatalogNotFound(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return repr(self.repo)

class RepositoryVerificationFailed(Exception):
    def __init__(self, message, repo):
        Exception.__init__(self, message)
        self.repo = repo

    def __str__(self):
        return self.args[0] + " (Repo: " + repr(self.repo) + ")"


class RepositoryIterator:
    """ Iterates through all directory entries in a whole Repository """

    class _CatalogIterator:
        def __init__(self, catalog):
            self.catalog          = catalog
            self.catalog_iterator = catalog.__iter__()


    def __init__(self, repository, catalog_hash=None):
        self.repository    = repository
        self.catalog_stack = collections.deque()
        if catalog_hash is None:
            catalog = repository.retrieve_root_catalog()
        else:
            catalog = repository.retrieve_catalog(catalog_hash)
        self._push_catalog(catalog)


    def __iter__(self):
        return self


    def next(self):
        full_path, dirent = self._get_next_dirent()
        if dirent.is_nested_catalog_mountpoint():
            self._fetch_and_push_catalog(full_path)
            return self.next() # same directory entry is also in nested catalog
        return full_path, dirent


    def _get_next_dirent(self):
        try:
            return self._get_current_catalog().catalog_iterator.next()
        except StopIteration, e:
            self._pop_catalog()
            if not self._has_more():
                raise StopIteration()
            return self._get_next_dirent()


    def _fetch_and_push_catalog(self, catalog_mountpoint):
        current_catalog = self._get_current_catalog().catalog
        nested_ref      = current_catalog.find_nested_for_path(catalog_mountpoint)
        if not nested_ref:
            raise NestedCatalogNotFound(self.repository)
        new_catalog     = nested_ref.retrieve_from(self.repository)
        self._push_catalog(new_catalog)


    def _has_more(self):
        return len(self.catalog_stack) > 0


    def _push_catalog(self, catalog):
        catalog_iterator = self._CatalogIterator(catalog)
        self.catalog_stack.append(catalog_iterator)

    def _get_current_catalog(self):
        return self.catalog_stack[-1]

    def _pop_catalog(self):
        return self.catalog_stack.pop()


class CatalogTreeIterator:
    class _CatalogWrapper:
        def __init__(self, repository):
            self.repository        = repository
            self.catalog           = None
            self.catalog_reference = None

        def get_catalog(self):
            if self.catalog == None:
                self.catalog = self.catalog_reference.retrieve_from(self.repository)
            return self.catalog

    def __init__(self, repository, root_catalog):
        if not root_catalog:
            root_catalog = repository.retrieve_root_catalog()
        self.repository    = repository
        self.catalog_stack = collections.deque()
        wrapper            = self._CatalogWrapper(self.repository)
        wrapper.catalog    = root_catalog
        self._push_catalog_wrapper(wrapper)

    def __iter__(self):
        return self

    def next(self):
        if not self._has_more():
            raise StopIteration()
        catalog = self._pop_catalog()
        self._push_nested_catalogs(catalog)
        return catalog

    def _has_more(self):
        return len(self.catalog_stack) > 0

    def _push_nested_catalogs(self, catalog):
        for nested_reference in catalog.list_nested():
            wrapper = self._CatalogWrapper(self.repository)
            wrapper.catalog_reference = nested_reference
            self._push_catalog_wrapper(wrapper)

    def _push_catalog_wrapper(self, catalog):
        self.catalog_stack.append(catalog)

    def _pop_catalog(self):
        wrapper = self.catalog_stack.pop()
        return wrapper.get_catalog()



class Cache:
    def __init__(self, cache_dir):
        if not os.path.exists(cache_dir):
            cache_dir = tempfile.mkdtemp(dir='/tmp', prefix='cache.')
        self._cache_dir = cache_dir
        self._create_cache_structure()
        
    def _create_dir(self, path):
        cache_full_path = '/'.join([self._cache_dir, path])
        if not os.path.exists(cache_full_path):
            os.mkdir(cache_full_path, 0755)
            
    def _create_cache_structure(self):
        self._create_dir('data')
        for i in range(0x00, 0xff + 1):
            new_folder = '{0:#0{1}x}'.format(i, 4)[2:]
            self._create_dir('data' + '/' + new_folder)
       
    def add(self, file_name):
        full_path = '/'.join([self._cache_dir, file_name])
        return open(full_path, 'w+')
    
    def get(self, file_name):
        full_path = '/'.join([self._cache_dir, file_name])
        if os.path.exists(full_path):
            return open(full_path, "rb")
        return None
    
    def evict(self):
        for i in range(0x00, 0xff + 1):
            folder = '{0:#0{1}x}'.format(i, 4)[2:]
            wildcard = '/'.join([self._cache_dir, folder, '*'])
            os.remove(glob.glob(wildcard))


class Fetcher:
    """ Abstract wrapper around a Fetcher """
    
    __metadata__ = abc.ABCMeta
    
    def __init__(self, cache_dir=''):
        self.cache = Cache(cache_dir)
    
    @abc.abstractmethod
    def retrieve_file(self, file_name):
        """ Abstract method to retrieve a file from the repository """
        pass


class LocalFetcher(Fetcher):
    """ Retrieves files ONLY from the local cache """
    def __init__(self, cache_dir=''):
        Fetcher.__init__(self, cache_dir)
    
    def retrieve_file(self, file_name, decompress=False):
        cached_file = self.cache.get(file_name)
        if not cached_file:
            raise FileNotFoundInRepository(self, file_name)
        return cached_file
        
    
class RemoteFetcher(Fetcher):
    """ Retrieves files from the local cache if found, and from remote otherwise """


    def __init__(self, repo_url, cache_dir=""):
        Fetcher.__init__(self, cache_dir)
        self._repo_url = repo_url


    def _download_content_and_store(self, cached_file, file_url):
        response = requests.get(file_url, stream=True)
        if response.status_code != requests.codes.ok:
            raise FileNotFoundInRepository(self, file_url)
        for chunk in response.iter_content(chunk_size=4096):
            if chunk:
                cached_file.write(chunk)
        cached_file.seek(0)
        cached_file.flush()
        return cached_file


    def _download_content_and_decompress(self, cached_file, file_url):
        response = requests.get(file_url, stream=False)
        if response.status_code != requests.codes.ok:
            raise FileNotFoundInRepository(self, file_url)
        decompressed_content = zlib.decompress(response.text)
        cached_file.write(decompressed_content)
        cached_file.seek(0)
        cached_file.flush()
        return cached_file


    def _retrieve_file(self, file_name, decompress):
        file_url = '/'.join([self._repo_url, file_name])
        cached_file = self.cache.add(file_name)
        if not decompress:
            return self._download_content_and_store(cached_file, file_url)
        else:
            return self._download_content_and_decompress(cached_file, file_url)


    def retrieve_file(self, file_name, decompress=False):
        cached_file = self.cache.get(file_name)
        if cached_file:
            if not decompress:
                return cached_file
        return self._retrieve_file(file_name, decompress)
            

class Repository:
    """ Wrapper around a CVMFS Repository representation """


    def __init__(self, cache_dir='', repo_url=''):
        if repo_url == '' and cache_dir == '':
            raise Exception("repo_url and cache_dir cannot be empty at the same time")
        if repo_url == '':
            self._fetcher = LocalFetcher(cache_dir)
        else:
            self._fetcher = RemoteFetcher(repo_url, cache_dir)
        self._storage_location = self._fetcher.cache._cache_dir
        self._opened_catalogs = {}
        #self.retrieve_file(".cvmfspublished", decompress=False)
        self._read_manifest()
        self._try_to_get_last_replication_timestamp()
        self._try_to_get_replication_state()


    def __iter__(self):
        return RepositoryIterator(self)


    def _read_manifest(self):
        try:
            with self.retrieve_file(_common._MANIFEST_NAME) as manifest_file:
                self.manifest = Manifest(manifest_file)
            self.fqrn = self.manifest.repository_name
        except FileNotFoundInRepository, e:
            raise RepositoryNotFound(self._storage_location)


    @staticmethod
    def __read_timestamp(timestamp_string):
        return dateutil.parser.parse(timestamp_string)


    def _try_to_get_last_replication_timestamp(self):
        try:
            with self.retrieve_file(_common._LAST_REPLICATION_NAME) as rf:
                timestamp = rf.readline()
                self.last_replication = Repository.__read_timestamp(timestamp)
            if not self.has_repository_type():
                self.type = 'stratum1'
        except FileNotFoundInRepository, e:
            self.last_replication = datetime.fromtimestamp(0, tz=tzutc())


    def _try_to_get_replication_state(self):
        self.replicating = False
        try:
            with self.retrieve_file(_common._REPLICATING_NAME) as rf:
                timestamp = rf.readline()
                self.replicating = True
                self.replicating_since = Repository.__read_timestamp(timestamp)
        except FileNotFoundInRepository, e:
            pass


    def verify(self, public_key_path):
        whitelist   = self.retrieve_whitelist()
        certificate = self.retrieve_certificate()
        if not whitelist.verify_signature(public_key_path):
            raise RepositoryVerificationFailed("Public key doesn't fit", self)
        if whitelist.expired():
            raise RepositoryVerificationFailed("Whitelist expired", self)
        if not whitelist.contains(certificate):
            raise RepositoryVerificationFailed("Certificate not in whitelist", self)
        if not self.manifest.verify_signature(certificate):
            raise RepositoryVerificationFailed("Certificate doesn't fit", self)
        return True


    def catalogs(self, root_catalog = None):
        return CatalogTreeIterator(self, root_catalog)


    def has_repository_type(self):
        return hasattr(self, 'type') and self.type != 'unknown'


    def has_history(self):
        return self.manifest.has_history()


    def retrieve_history(self):
        if not self.has_history():
            raise HistoryNotFound(self)
        history_db = self.retrieve_object(self.manifest.history_database, 'H')
        return History(history_db)


    def retrieve_whitelist(self):
        whitelist = self.retrieve_file(_common._WHITELIST_NAME)
        return Whitelist(whitelist)


    def retrieve_certificate(self):
        certificate = self.retrieve_object(self.manifest.certificate, 'X')
        return Certificate(certificate)


    def retrieve_file(self, file_name, decompress=False):
        """ Method to retrieve a file from the repository """
        return self._fetcher.retrieve_file(file_name)


    def retrieve_object(self, object_hash, hash_suffix = ''):
        """ Retrieves an object from the content addressable storage """
        path = "data/" + object_hash[:2] + "/" + object_hash[2:] + hash_suffix
        return self.retrieve_file(path)


    def retrieve_root_catalog(self):
        return self.retrieve_catalog(self.manifest.root_catalog)


    def retrieve_catalog_for_path(self, needle_path):
        """ Recursively walk down the Catalogs and find the best fit for a path """
        clg = self.retrieve_root_catalog()
        nested_reference = None
        while True:
            new_nested_reference = clg.FindNestedForPath(needle_path)
            if new_nested_reference == None:
                break
            nested_reference = new_nested_reference
            clg = self.retrieve_catalog(nested_reference.hash)
        return clg


    def close_catalog(self, catalog):
        try:
            open_catalog = self._opened_catalogs[catalog.hash]
            del self._opened_catalogs[catalog.hash]
        except KeyError, e:
            print "not found:" , catalog.hash
            pass


    def retrieve_catalog(self, catalog_hash):
        """ Download and open a catalog from the repository """
        if catalog_hash in self._opened_catalogs:
            return self._opened_catalogs[catalog_hash]
        else:
            return self._retrieve_and_open_catalog(catalog_hash)

    def _retrieve_and_open_catalog(self, catalog_hash):
        catalog_file = self.retrieve_object(catalog_hash, 'C')
        new_catalog = Catalog(catalog_file, catalog_hash)
        self._opened_catalogs[catalog_hash] = new_catalog
        return new_catalog


def all_local():
    d = _common._REPO_CONFIG_PATH
    if not os.path.isdir(d):
        raise _common.CvmfsNotInstalled
    return [ LocalRepository(repo) for repo in os.listdir(d) if os.path.isdir(os.path.join(d, repo)) ]

def all_local_stratum0():
    return [ repo for repo in all_local() if repo.type == 'stratum0' ]

def open_repository(repository_path, public_key = None):
    repo = RemoteRepository(repository_path)              \
                if repository_path.startswith("http://")  \
                else LocalRepository(repository_path)
    if public_key and not repo.verify(public_key):
        return None
    return repo
