import cvmfs
import time
import sys
from balance import *
from catalog_mocker import *

balancing_threshold = 50000
overflow_threshold = balancing_threshold * 2
underflow_threshold = 1000
max_iterations = int(sys.argv[2]) - 1
repository = str(sys.argv[1])
repo_cache = repository
#repo_url = "http://cvmfs-stratum-zero.cern.ch/cvmfs/" + repository


def get_entries(repo):
    entries = []
    for catalog in repo.catalogs():
        entries.append(catalog.get_statistics().num_entries())
    return entries


def get_catalog_statistics(repo, catalog_hash, max_catalog_size, iteration):
    root_catalog = repo.retrieve_catalog(catalog_hash)
    statistics = root_catalog.get_statistics()
    entries = get_entries(repo)
    total_tree_size = statistics.num_subtree_entries()
    # iteration, size, time, num_catalogs, min_catalog, mean, median, max,
    # min, std_deviation
    return                                              \
        iteration,                                      \
        total_tree_size,                                \
        0,                                              \
        len(entries),                                   \
        float(total_tree_size) / max_catalog_size,      \
        numpy.mean(entries),                            \
        numpy.median(entries),                          \
        numpy.max(entries),                             \
        numpy.min(entries),                             \
        numpy.std(entries)


def build_virtual_tree(repo, catalog_hash):
    virtual_tree = {}
    for full_path, dirent in cvmfs.RepositoryIterator(repo, catalog_hash):
        virtual_node = VirtualNode(full_path, dirent.is_directory(), False)
        virtual_tree[full_path] = virtual_node
    for path in virtual_tree:
        parent_path = path.rsplit("/", 1)[0]
        if path != "":
            virtual_tree[parent_path].children.append(path)
    return virtual_tree


def extract_entries_from_catalog(catalog):
    virtual_subtree = {}
    for full_path, dirent in catalog:
        virtual_node = VirtualNode(full_path, dirent.is_directory(), False)
        virtual_subtree[full_path] = virtual_node
    return virtual_subtree


def root_catalog_history(repo, num_revisions):
    root_catalog = repo.retrieve_root_catalog()
    catalogs = [root_catalog]
    for _ in range(1, num_revisions):
        predecessor_ref = root_catalog.get_predecessor()
        predecessor = repo.retrieve_catalog(predecessor_ref.hash)
        catalogs.append(predecessor)
        root_catalog = predecessor
    catalogs.reverse()
    return catalogs


def get_revision_changes(repo, changed_catalog_hashes):
    changes = []
    for catalog_hash in changed_catalog_hashes:
        new_catalog = repo.retrieve_catalog(catalog_hash)
        new_catalog_entries = \
            set((full_path, dirent) for full_path, dirent in new_catalog)
        old_catalog_ref = new_catalog.get_predecessor()
        old_catalog_entries = set()
        print "NEW:", new_catalog.hash, new_catalog.revision, new_catalog.root_prefix
        if old_catalog_ref and int(new_catalog.revision) > 1:
            try:
                old_catalog = repo.retrieve_catalog(old_catalog_ref.hash)
                print "OLD:", old_catalog.hash, old_catalog.revision, old_catalog.root_prefix
                old_catalog_entries = \
                    set((full_path, dirent) for full_path, dirent in old_catalog)
            except Exception:
                print "Failed to retrieve the catalog ", old_catalog_ref.hash, \
                      "which is the previous revision of ", catalog_hash
        old_catalog_pathes = set(full_path for full_path, dirent in
                                 old_catalog_entries)
        new_catalog_pathes = set(full_path for full_path, dirent in
                                 new_catalog_entries)
        if len(old_catalog_entries) > 0:
            deletion_pathes = old_catalog_pathes - new_catalog_pathes
            insertion_pathes = new_catalog_pathes - old_catalog_pathes
        else:
            deletion_pathes = set()
            insertion_pathes = new_catalog_pathes

        additions = set((full_path, dirent) for full_path, dirent
                        in new_catalog_entries if full_path in insertion_pathes)
        changes.append((deletion_pathes, additions))
        print "Added:", len(additions), "   Removed:", len(deletion_pathes)
    return changes


def apply_changes(changes, balancer):
    num_deletions = 0
    num_additions = 0
    for deletions, additions in changes:
        for full_path in deletions:
            balancer.delete_node(full_path)
            num_deletions += 1
        for full_path, dirent in additions:
            virtual_node = VirtualNode(full_path, dirent.is_directory())
            balancer.insert_node(virtual_node)
            num_additions += 1
    return num_deletions, num_additions


def main():
    print "Generating statistics for the original repository"
    original_file_path = "/tmp/original_repo.csv"
    f_orig = open(original_file_path, "w")
    f_orig.write(statistics_header() + "\n")
    balanced_file_path = "/tmp/balanced_repo.csv"
    f_mod = open(balanced_file_path, "w")
    f_mod.write(statistics_header() + "\n")

    print "\n\nINITIAL ITERATION"
    #repo = cvmfs.RemoteRepository(repo_url)
    repo = cvmfs.LocalRepository(repo_cache)
    root_catalogs = root_catalog_history(repo, max_iterations + 1)
    base_revision = root_catalogs.pop(0)
    old_hashes = set(c.hash for c in repo.catalogs(base_revision))

    virtual_tree = build_virtual_tree(repo, base_revision.hash)
    balancer = VirtualBalancer(balancing_threshold/2, balancing_threshold)
    balancer.virtual_tree = virtual_tree
    balancer.balance()
    iteration = 0

    for original_root_catalog in root_catalogs:
        print "\n"
        iteration += 1
        catalog_hash = original_root_catalog.hash
        print "Iteration ", iteration
        # part 1: original repository
        original_statistics = get_catalog_statistics(repo, catalog_hash,
                                                     balancing_threshold, 0)
        original_statistics_formatted = \
            ";".join(map(str, original_statistics)).replace(".", ",")
        f_orig.write(original_statistics_formatted)
        f_orig.flush()

        # part 2: my virtual repository with applied changes
        start_time = time.time()
        new_hashes = set(c.hash for c in repo.catalogs(original_root_catalog))
        changed_hashes = new_hashes - old_hashes
        changes = get_revision_changes(repo, changed_hashes)
        num_deletions, num_additions = apply_changes(changes, balancer)
        print "Additions:", num_additions, "   Deletions:", num_deletions, "\n"
        balancer.full_rebalance(underflow_threshold, overflow_threshold)
        total_time = time.time() - start_time

        list_weights = balancer.summary(print_tree=False)
        statistics_str = collect_statistics(balancer, balancing_threshold,
                                            list_weights, 0, total_time)
        f_mod.write(statistics_str + "\n")
        f_mod.flush()
        old_hashes = new_hashes

    f_orig.close()
    f_mod.close()

if __name__ == "__main__":
    main()

