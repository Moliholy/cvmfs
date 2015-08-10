from os import listdir, sep
from os.path import abspath, basename, isdir
from shutil import rmtree
from balance import Balancer, VirtualBalancer, TreeModificator

import argparse
import tempfile
import time
import numpy


def visualize_tree(directory, padding=' '):
    print padding[:-1] + '+-' + basename(abspath(directory)) + '/'
    padding += ' '
    files = listdir(directory)
    count = 0
    for f in files:
        count += 1
        print padding + '|'
        path = directory + sep + f
        if isdir(path):
            if count == len(files):
                visualize_tree(path, padding + ' ')
            else:
                visualize_tree(path, padding + '|')
        else:
            print padding + '+-' + f


def parse_args():
    argparser = argparse.ArgumentParser()
    group = argparser.add_argument_group("common")
    command_parser = argparser.add_subparsers(help="commands")

    # options for the 'generate' command
    generate_cmd = command_parser.add_parser("generate",
                                             help="Generate a test tree")
    generate_cmd.add_argument("--root_directory",
                              default=None,
                              required=False, type=str,
                              help="Directory to analyze")
    generate_cmd.add_argument("--num_entries",
                              default=150000,
                              required=False, type=int,
                              help="Number of nodes to generate in the tree")
    generate_cmd.set_defaults(command="generate")

    # options for the 'benchmark_modification' command
    benchmark_modification_cmd = command_parser.add_parser(
        "benchmark_modification",
        help="Create a benchmark")
    benchmark_modification_cmd.add_argument("--max_chunk",
                                            default=5000,
                                            required=False, type=int,
                                            help="Maximum adition or deletion")
    benchmark_modification_cmd.add_argument("--tree_size",
                                            default=100000,
                                            required=False, type=int,
                                            help="Initial size of the tree")
    benchmark_modification_cmd.add_argument("--iterations",
                                            default=100,
                                            required=False, type=int,
                                            help="Number of iterations")
    benchmark_modification_cmd.add_argument("--output_file",
                                            default="/tmp/modifications.csv",
                                            required=False, type=str,
                                            help="Final output file")
    benchmark_modification_cmd.set_defaults(command="benchmark_modification")

    # options for the 'benchmark' command
    benchmark_cmd = command_parser.add_parser("benchmark",
                                              help="Create a benchmark")
    benchmark_cmd.add_argument("--virtualized",
                               default=False,
                               action="store_true",
                               required=False,
                               help="Virtualize the process")
    benchmark_cmd.add_argument("--sequence",
                               default="50000,1000000,50000",
                               required=False, type=str,
                               help="""Comma-separated list of options.
                               1) Initial size of the generated tree
                               2) Maximum size of the generated tree
                               3) Per-iteration jump

                               For example: 3,6,2     10,100,10""")
    benchmark_cmd.add_argument("--output_file",
                               default="/tmp/catalog_benchmark.csv",
                               required=False, type=str,
                               help="Final output file")
    benchmark_cmd.set_defaults(command="benchmark")

    # common options
    group.add_argument("--optimal_weight",
                       default=10000,
                       required=False, type=int,
                       help="Optimal number of entries in a catalog")
    group.add_argument("--max_weight",
                       default=20000,
                       required=False, type=int,
                       help="Maximum number of entries in a catalog")
    return argparser.parse_args()


def collect_statistics(balancer, max_weight, list_weights, iteration,
                       execution_time, balance=0):
    separator = ";"
    formatter = "{0:.3f}"
    min_catalogs = float(balancer.size()) / float(max_weight)
    total_entries = balancer.catalog_tree_root.total_entries()
    to_print = separator.join([
        str(iteration),
        str(balancer.size()),
        str(formatter.format(execution_time, 4)),
        str(balance),
        str(len(list_weights)),
        str(formatter.format(min_catalogs)),
        str(total_entries),
        str(formatter.format(numpy.mean(list_weights), 4)),
        str(numpy.median(list_weights)),
        str(numpy.max(list_weights)),
        str(numpy.min(list_weights)),
        str(formatter.format(numpy.std(list_weights), 4))
    ]).replace(".", ",")
    return to_print


def statistics_header():
    separator = ";"
    return separator.join([
                          "iteration",
                          "size",
                          "time",
                          "balance",
                          "catalog",
                          "min_catalog",
                          "num_entries",
                          "mean",
                          "median",
                          "max",
                          "min",
                          "std_deviation"])


def benchmark_modification(args):
    output_file = open(args.output_file, "w")
    header = statistics_header()
    output_file.write(header + "\n")
    catalog_initial_size = int(args.tree_size * 0.05)
    catalog_max_size = 2 * catalog_initial_size
    catalog_min_size = catalog_max_size / 8
    modifications_size = int(catalog_max_size * 0.1)
    max_modification_chunk = int(catalog_initial_size)

    # first iteration is the creation and it's special
    balancer = VirtualBalancer(catalog_initial_size / 2, catalog_initial_size)
    balancer.generate_self_virtual_tree(args.tree_size)
    start_time = time.time()
    balancer.balance()
    end_time = time.time()
    total_time = end_time - start_time
    list_weights = balancer.summary(print_tree=True)
    statistics_str = collect_statistics(balancer, catalog_initial_size,
                                        list_weights, 0, total_time)
    output_file.write(statistics_str + "\n")
    tree_modificator = TreeModificator(balancer)

    # iterations to modify the tree from now
    for iteration in range(1, args.iterations + 1):
        start_time = time.time()
        balance = tree_modificator.simulation(modifications_size,
                                              max_modification_chunk,
                                              catalog_min_size,
                                              catalog_max_size)
        end_time = time.time()
        total_time = end_time - start_time
        list_weights = balancer.summary(print_tree=False)
        statistics_str = collect_statistics(balancer, catalog_max_size,
                                            list_weights, iteration,
                                            total_time, balance)
        output_file.write(statistics_str + "\n")
        max_catalog = max(balancer.catalog_list.values(), key=lambda x: x.weight)
        print "MAXIMUM CATALOG in \'" + max_catalog.relative_path + \
              "\' with weight " + str(max_catalog.weight) + \
              " < " + str(catalog_max_size) + "\n"
    output_file.close()


def benchmark(args):
    iteration = 0
    steps_list = args.sequence.split(",")
    initial_tree_size = int(steps_list[0])
    max_tree_size = int(steps_list[1])
    jump = int(steps_list[2])
    output_file = open(args.output_file, "w")
    header = statistics_header()
    output_file.write(header + "\n")

    for size in range(initial_tree_size, max_tree_size + jump, jump):
        iteration += 1
        balancer = VirtualBalancer(args.optimal_weight, args.max_weight)
        balancer.generate_self_virtual_tree(size)
        entries = balancer.virtual_tree[""].check_tree()
        print("Number of entries = " + str(entries))
        start_time = time.time()
        balancer.balance()
        end_time = time.time()
        total_time = end_time - start_time

        list_weights = balancer.summary(print_tree=True)
        print("\n\n\n")
        to_print = collect_statistics(balancer, args.max_weight,
                                      list_weights, iteration, total_time)
        output_file.write(to_print + "\n")

    output_file.close()


def generate(args):
    directory = args.root_directory
    fs_tree_created = False
    if directory is None:
        directory = tempfile.mkdtemp(prefix="catalog.", dir="/tmp")
        Balancer.create_test_fs_tree(directory, args.num_entries)
        fs_tree_created = True

    start_time = time.time()
    balancer = Balancer(directory, args.optimal_weight, args.max_weight)
    balancer.balance()
    end_time = time.time()

    if args.num_entries < 150:
        print("\n\n\n===================== TREE ==================== \n\n")
        visualize_tree(directory)
    else:
        print("Not printing the tree (too many nodes)\n\n")
    print("\n\n\n=================== SUMMARY =================== \n\n")
    num_catalog_files = balancer.summary(print_tree=True)
    print("\n" + str(end_time - start_time) + " seconds\n" +
          str(args.num_entries) + " nodes in the file system tree\n" +
          str(num_catalog_files) + " \'.cvmfscatalog\' files created\n" +
          str(args.optimal_weight) + " optimal entry size\n" +
          str(args.max_weight) + " maximum entry size\n")

    if fs_tree_created:
        rmtree(directory)


def main():
    args = parse_args()
    if args.command == "benchmark":
        benchmark(args)
    if args.command == "generate":
        generate(args)
    if args.command == "benchmark_modification":
        benchmark_modification(args)


if __name__ == "__main__":
    main()
