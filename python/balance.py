import os
import random


class CatalogNode:
    def __init__(self, relative_path, parent_node):
        self.relative_path = relative_path
        self.parent_node = parent_node
        self.children = {}
        self.weight = 0
        self.allows_underflow = False

    def is_root_catalog(self):
        return self.parent_node is None

    def expand(self, directory):
        file_list = os.listdir(directory)
        self.weight += len(file_list)
        return file_list

    def virtual_expand(self, virtual_node):
        file_list = virtual_node.children
        self.weight += len(file_list)
        return file_list

    def add_child(self, relative_path):
        child = None
        if isinstance(relative_path, str):
            child = CatalogNode(relative_path, self)
        elif isinstance(relative_path, CatalogNode):
            child = relative_path
        self.children[relative_path] = child
        return child

    def remove_child(self, relative_path, absolute_path=None):
        if relative_path in self.children:
            child = self.children.pop(relative_path)
            for grandchild in child.children:
                child.remove_child(grandchild.relative_path, absolute_path)
            self.weight += child.weight
            if absolute_path is not None:
                cvmfscatalog_path = absolute_path + child.relative_path \
                    + "/.cvmfscatalog"
                if os.path.isfile(cvmfscatalog_path):
                    os.remove(cvmfscatalog_path)

    def is_leaf(self):
        return len(self.children) == 0

    def total_entries(self):
        entries = self.weight
        for child in self.children:
            entries += child.total_entries()
        return entries


class Balancer(object):
    def __init__(self, root_path, optimal_weight, max_weight):
        self.root_path = root_path
        self.catalog_tree_root = CatalogNode("", None)
        self.optimal_weight = optimal_weight
        self.max_weight = max_weight

    def make_absolute_path(self, relative_path):
        return self.root_path + relative_path

    def __add_fs_subtree(self, catalog_node, relative_path):
        absolute_path = self.make_absolute_path(relative_path)
        for fsnode in catalog_node.expand(absolute_path):
            if os.path.isdir(absolute_path + "/" + fsnode):
                relative_fsnode_path = relative_path + "/" + fsnode
                if catalog_node.weight < self.optimal_weight:
                    self.__add_fs_subtree(catalog_node, relative_fsnode_path)
                else:
                    child = catalog_node.add_child(relative_fsnode_path)
                    self.balance(child)

    def __create_catalog_files(self, catalog_node):
        absolute_path = self.make_absolute_path(catalog_node.relative_path)
        if not catalog_node.is_root_catalog():
            open(absolute_path + "/.cvmfscatalog", "w").close()
        for child in catalog_node.children.values():
            self.__create_catalog_files(child)

    def __fix_catalog_tree(self, catalog_node):
        catalog_removed = False
        if catalog_node.is_leaf() and not catalog_node.is_root_catalog():
            if catalog_node.weight < self.optimal_weight:
                total_sum = catalog_node.weight + \
                    catalog_node.parent_node.weight
                if total_sum <= self.max_weight:
                    catalog_node.parent_node.remove_child(
                        catalog_node.relative_path)
                    catalog_removed = True
        else:
            for child in catalog_node.children.values():
                result = self.__fix_catalog_tree(child)
                if result:
                    catalog_removed = True
        return catalog_removed

    def _cut_leaves(self):
        result = True
        while result:
            result = self.__fix_catalog_tree(self.catalog_tree_root)

    def summary(self, separator="", catalog_node=None, print_tree=False):
        if catalog_node is None:
            catalog_node = self.catalog_tree_root
        if print_tree:
            print(separator + catalog_node.relative_path +
                  " - " + str(catalog_node.weight))
        separator += " "
        weight_list = [catalog_node.weight]
        for child in catalog_node.children.values():
            weight_list += self.summary(separator, child, print_tree)
        return weight_list

    def reset(self):
        for child_path in self.catalog_tree_root.children:
            self.catalog_tree_root.remove_child(child_path, self.root_path)
        self.catalog_tree_root.weight = 0

    def balance(self, catalog_node=None):
        if catalog_node is None:
            catalog_node = self.catalog_tree_root
        self.__add_fs_subtree(catalog_node, catalog_node.relative_path)
        if catalog_node.is_root_catalog():
            self._cut_leaves()
            self.__create_catalog_files(self.catalog_tree_root)

    @staticmethod
    def create_test_fs_tree(root_directory, num_entries, directories=None,
                            num_files=0, num_directories=0):
        if directories is None or len(directories) == 0:
            directories = [root_directory]
        while num_directories + num_files < num_entries:
            curr_dir = directories[random.randint(0, num_directories)]
            if random.randint(0, 3) == 0:  # 25% chance to be a directory
                num_directories += 1
                newdir_path = curr_dir + "/d" + str(num_directories)
                os.mkdir(newdir_path, 0777)
                directories.append(newdir_path)
            else:
                num_files += 1
                open(curr_dir + "/f" + str(num_files), "w").close()
        return [directories, num_files, num_directories]


class VirtualNode:
    def __init__(self, path, is_directory, is_catalog=False):
        self.is_directory = is_directory
        self.path = path
        self.weight = 1
        if self.is_directory:
            self.children = []
            self.is_catalog = is_catalog

    def calculate_weight(self, virtual_tree):
        if (self.is_directory and self.is_catalog) or not self.is_directory:
            self.weight = 1
        else:
            self.weight = 1
            for child_ref in self.children:
                if virtual_tree[child_ref].is_directory and not \
                        virtual_tree[child_ref].is_catalog:
                    self.weight += virtual_tree[child_ref].weight
                else:
                    self.weight += 1

    def get_weight(self):
        if (self.is_directory and self.is_catalog) or not self.is_directory:
            return 1
        return self.weight


class VirtualBalancer(Balancer):
    def __init__(self, optimal_weight, maximum_weight):
        Balancer.__init__(self, "", optimal_weight, maximum_weight)
        self.virtual_tree = {}
        self.entries_left = 0
        self.catalog_list = {}

    def size(self):
        return len(self.virtual_tree)

    @staticmethod
    def generate_virtual_tree(num_entries, mountpoint, prefix=""):
        num_files = 0
        num_directories = 1
        virtual_tree = {mountpoint.path: mountpoint}
        directories = virtual_tree.values()
        while num_directories + num_files < num_entries:
            curr_dir = directories[random.randint(0, num_directories - 1)]
            if random.randint(0, 3) == 0:  # 25% chance to be a directory
                num_directories += 1
                path = curr_dir.path + "/" + prefix + "d" + \
                    str(num_directories)
                new_node = VirtualNode(path, True)
                directories.append(new_node)
            else:
                num_files += 1
                path = curr_dir.path + "/" + prefix + "f" + str(num_files)
                new_node = VirtualNode(path, False)
            curr_dir.children.append(new_node.path)
            virtual_tree[path] = new_node
        return virtual_tree

    def generate_self_virtual_tree(self, num_entries, mountpoint=None):
        if mountpoint is None:
            mountpoint = VirtualNode("", True, False)
        self.virtual_tree = VirtualBalancer.generate_virtual_tree(num_entries,
                                                                  mountpoint)
        self.entries_left += num_entries

    def __create_catalog_files(self, catalog_node):
        node = self.virtual_tree[catalog_node.relative_path]
        if not catalog_node.is_root_catalog():
            path = node.path + "/.cvmfscatalog"
            new_node = VirtualNode(path, False)
            node.children.append(new_node.path)
            self.virtual_tree[new_node.path] = new_node
        for child in catalog_node.children.values():
            self.__create_catalog_files(child)

    def find_catalog(self, path):
        found = False
        parent_path = str(path)
        while not found and parent_path != "":
            last_index = parent_path.rfind("/")
            parent_path = parent_path[:last_index]
            if parent_path in self.catalog_list:
                found = True
        return parent_path

    @staticmethod
    def get_parent_path(path):
        last_index = path.rfind("/")
        if last_index == 0:
            return ""
        return path[:last_index]

    def find_parent_node(self, path):
        parent_path = VirtualBalancer.get_parent_path(path)
        if parent_path in self.virtual_tree:
            return self.virtual_tree[parent_path]

    def rebuild_catalog_tree(self):
        for catalog in self.catalog_list.values():
            catalog.children.clear()  # previous clean-up
        for catalog in self.catalog_list.values():
            if catalog is self.catalog_tree_root:
                continue
            parent_path = self.find_catalog(catalog.relative_path)
            if parent_path in self.catalog_list:
                parent_catalog = self.catalog_list[parent_path]
                parent_catalog.add_child(catalog)

    def balance(self, virtual_root_node=None):
        if virtual_root_node is None:
            virtual_root_node = self.virtual_tree[""]
        virtual_root_node.is_catalog = False
        self.optimal_partition(virtual_root_node)
        self.add_catalog(None, virtual_root_node)
        self.catalog_tree_root = self.catalog_list[""]
        self.rebuild_catalog_tree()

    def remove_catalog(self, path):
        if path in self.virtual_tree and path in self.catalog_list:
            self.virtual_tree[path].is_catalog = False
            removed_catalog = self.catalog_list.pop(path)
            parent_catalog_path = self.find_catalog(path)
            if parent_catalog_path in self.catalog_list:
                parent_catalog = self.catalog_list[parent_catalog_path]
                if removed_catalog.weight > 0:
                    parent_catalog.weight += removed_catalog.weight
                return parent_catalog
            self.rebuild_catalog_tree()
        return None

    def optimal_partition(self, virtual_node):
        # postorder track of the tree
        for child_ref in virtual_node.children:
            virtual_child = self.virtual_tree[child_ref]
            if virtual_child.is_directory and not virtual_child.is_catalog:
                self.optimal_partition(virtual_child)
        virtual_node.calculate_weight(self.virtual_tree)
        while virtual_node.weight > self.max_weight:
            heaviest_node = VirtualBalancer.max_child(virtual_node,
                                                      self.virtual_tree)
            self.add_catalog(virtual_node, heaviest_node)

    def add_catalog(self, parent_node, child_node):
        child_node.is_catalog = True
        if parent_node is not None:
            parent_node.calculate_weight(self.virtual_tree)
        new_catalog = CatalogNode(child_node.path, None)
        new_catalog.weight = child_node.weight - 1
        if new_catalog.weight > 0:
            self.catalog_list[new_catalog.relative_path] = new_catalog
            print "NEW CATALOG in \'" + new_catalog.relative_path + \
                  "\' with weight " + str(new_catalog.weight)
            self.entries_left -= new_catalog.weight

    def insert_node(self, virtual_node):
        #print "INSERTING NODE in ", virtual_node.path
        if virtual_node.path != "":
            parent_node = self.find_parent_node(virtual_node.path)
            parent_path = VirtualBalancer.get_parent_path(virtual_node.path)
            if parent_node is None and parent_path != virtual_node.path:
                parent_node = VirtualNode(parent_path, True)
                self.insert_node(parent_node)
            if virtual_node.path not in parent_node.children:
                parent_node.children.append(virtual_node.path)
            if virtual_node.path not in self.virtual_tree:
                catalog_path = self.find_catalog(virtual_node.path)
                catalog = self.catalog_list[catalog_path]
                catalog.weight += 1
            self.virtual_tree[virtual_node.path] = virtual_node

    def weak_insertion(self, path):
        catalog_path = self.find_catalog(path)
        if catalog_path in self.catalog_list:
            catalog = self.catalog_list[catalog_path]
            catalog.weight += 1

    def weak_deletion(self, path):
        catalog_path = self.find_catalog(path)
        if catalog_path in self.catalog_list:
            catalog = self.catalog_list[catalog_path]
            catalog.weight -= 1

    def delete_node(self, node_path):
        #print "DELETING NODE IN ", node_path
        if node_path != "" and node_path in self.virtual_tree:
            virtual_node = self.virtual_tree[node_path]
            if virtual_node.is_directory:
                for child_ref in virtual_node.children:
                    self.delete_node(child_ref)
            catalog_path = self.find_catalog(virtual_node.path)
            catalog = self.catalog_list[catalog_path]
            parent_node = self.find_parent_node(virtual_node.path)
            if parent_node and virtual_node.path in parent_node.children:
                parent_node.children.remove(virtual_node.path)
            del self.virtual_tree[virtual_node.path]
            if virtual_node.path not in self.catalog_list:
                catalog.weight -= 1
            elif virtual_node.path != "":
                del self.catalog_list[virtual_node.path]

    @staticmethod
    def max_child(virtual_node, virtual_tree):
        max_child = None
        max_weight = 0
        if virtual_node.is_directory and not virtual_node.is_catalog:
            for child_ref in virtual_node.children:
                child = virtual_tree[child_ref]
                if child.is_directory and not \
                        child.is_catalog and \
                   max_weight < child.get_weight():
                        max_weight = child.get_weight()
                        max_child = child
        return max_child

    def rebalance(self, underflow_threshold, overflow_threshold, catalog_path):
        if catalog_path not in self.catalog_list:
            print "ERROR: ", catalog_path, \
                  "not in the catalog list, which is:\n", self.catalog_list
            return
        catalog = self.catalog_list[catalog_path]
        if catalog.weight < underflow_threshold:
            print "UNDERFLOW in the catalog located in ", catalog.relative_path, \
                  "with size", catalog.weight, "<", underflow_threshold
            parent_catalog = \
                self.remove_catalog(catalog.relative_path)
            if parent_catalog:
                # re-balancing again because there can be an overflow
                self.rebalance(underflow_threshold, overflow_threshold,
                               parent_catalog.relative_path)
        elif catalog.weight > overflow_threshold:
            print "OVERFLOW in the catalog located in ", catalog.relative_path, \
                  "with size ", catalog.weight, ">", overflow_threshold
            virtual_node = self.virtual_tree[catalog.relative_path]
            self.balance(virtual_node)

    def full_rebalance(self, underflow_threshold, overflow_threshold):
        for catalog_path in self.catalog_list.keys():
            self.rebalance(underflow_threshold, overflow_threshold,
                           catalog_path)



















class TreeModificator(object):
    def __init__(self, balancer):
        self.balancer = balancer
        self.subtrees = {}

    def _select_random_dir(self, dirs):
        found = False
        tree = self.balancer.virtual_tree
        directory = None
        while not found:
            path = dirs[random.randint(0, len(dirs))]
            directory = tree[path]
            found = directory.is_directory
        return directory

    def spread_insertion(self, size, max_chunk):
        curr_size = 0
        insertions = []
        tree = self.balancer.virtual_tree
        dirs = tree.keys()
        while curr_size < size:
            mountpoint = self._select_random_dir(dirs)
            chunk_size = random.randint(1, max_chunk)
            if chunk_size > size - curr_size:
                chunk_size = size - curr_size
            curr_size += chunk_size
            insertion = VirtualBalancer.generate_virtual_tree(chunk_size,
                                                              mountpoint,
                                                              "NEW_")
            insertions.append((mountpoint.path, insertion))
        return insertions

    def spread_deletion(self, size, max_chunk):
        curr_size = 0
        deletions = []
        tree = self.balancer.virtual_tree
        dirs = tree.keys()
        while curr_size < size:
            current_path = dirs[random.randint(0, len(dirs) - 1)]
            current_node = tree[current_path]
            if not current_node.weight <= max_chunk:
                continue
            curr_size += current_node.weight
            deletions.append(current_node)
        return deletions

    def simulation(self, size, max_chunk, min_catalog_size, max_catalog_size):
        if random.randint(0, 10) <= 7:
            insertions = self.spread_insertion(size, max_chunk)
            balance = self._publish_insertions(insertions, max_catalog_size)
        else:
            deletions = self.spread_deletion(size, max_chunk)
            balance = self._publish_deletions(deletions, min_catalog_size,
                                              max_catalog_size)
        self.balancer.rebuild_catalog_tree()
        return balance

    def _publish_insertions(self, insertions, max_catalog_size):
        """Insertions is a [(mountpoint, dict(path, VirtualNode))] array of tuples]
        """
        dirty_catalogs = {}
        balance = 0
        for mountpoint, node_dict in insertions:
            branch_size = len(node_dict)
            catalog_path = self.balancer.find_catalog(mountpoint)
            if catalog_path not in self.balancer.catalog_list:
                continue
            catalog = self.balancer.catalog_list[catalog_path]

            # add the subtree to the main tree and update the catalog weight
            print "INSERTING " + str(len(node_dict)) + " entries in \'" + \
                  mountpoint + "\' that depend on the catalog located in \'" + \
                  catalog.relative_path + "\' with old weight " + \
                  str(catalog.weight) + " and new weight " + \
                  str(catalog.weight + len(node_dict))
            balance += len(node_dict)
            self.balancer.virtual_tree.update(node_dict)
            self.balancer.entries_left += len(node_dict)
            catalog.weight += branch_size - 1
            if catalog_path not in dirty_catalogs:
                dirty_catalogs[catalog_path] = catalog
        for dirty_catalog in dirty_catalogs.values():
            if dirty_catalog.weight > max_catalog_size:
                print "CATALOG OVERFLOW in \'" + dirty_catalog.relative_path + \
                      "\' with size " + str(dirty_catalog.weight) + \
                      ". Balancing..."
                virtual_node = \
                    self.balancer.virtual_tree[dirty_catalog.relative_path]
                self.balancer.balance(virtual_node)
        return balance

    def _publish_deletions(self, deletions, min_catalog_size, max_catalog_size):
        dirty_catalogs = {}
        balance = 0
        total_removed = 0
        tree = self.balancer.virtual_tree
        for node in deletions:
            node_path = node.path
            if node_path not in tree or \
               node_path is self.balancer.catalog_tree_root:
                continue
            virtual_node = tree[node_path]
            branch_size = virtual_node.weight
            catalog_path = self.balancer.find_catalog(node_path)
            if catalog_path not in self.balancer.catalog_list or \
               catalog_path == "":
                continue
            catalog = self.balancer.catalog_list[catalog_path]

            # remove the node
            balance -= branch_size
            self.balancer.entries_left -= branch_size
            removed_count = self.balancer.remove_node(node_path)
            total_removed += removed_count
            catalog.weight -= branch_size
            if catalog_path not in dirty_catalogs:
                dirty_catalogs[catalog_path] = catalog
        print "REMOVING " + str(total_removed) + " entries from the tree"
        for dirty_catalog in dirty_catalogs.values():
            if dirty_catalog.weight < min_catalog_size:
                parent_catalog = \
                    self.balancer.remove_catalog(dirty_catalog.relative_path)
                if parent_catalog:
                    dirty_catalogs[parent_catalog.relative_path] = \
                        parent_catalog
                    print "CATALOG REMOVED in \'" + dirty_catalog.relative_path + \
                        "\' of size " + str(dirty_catalog.weight) + \
                        " < " + str(min_catalog_size) + \
                        " and MERGED with \'" + parent_catalog.relative_path + \
                        "\' with old size " + \
                        str(parent_catalog.weight - dirty_catalog.weight) + \
                        " and new size " + str(parent_catalog.weight)
            if dirty_catalog.weight > max_catalog_size:
                self.balancer.entries_left -= dirty_catalog.weight
                if dirty_catalog.relative_path in tree:
                    print "CATALOG RESIZED in \'" + dirty_catalog.relative_path + \
                          "\' with weight " + str(dirty_catalog.weight) + " > " + \
                          str(max_catalog_size)
                    node = tree[dirty_catalog.relative_path]
                    self.balancer.balance(node)
        return balance