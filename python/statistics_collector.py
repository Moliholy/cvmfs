import time
import glob
import argparse
from influxdb.influxdb08 import InfluxDBClient


class DatabaseCreationException(Exception):
    pass


class DatabaseUpdateException(Exception):
    pass


class DatabaseGraphGenerationException(Exception):
    pass


class GraphCreationException(Exception):
    pass


class Counter:
    def __init__(self, name, description):
        self.name = name
        self.numbers = []
        self.description = description

    def count(self):
        return len(self.numbers)

    def add(self, number):
        self.numbers.append(int(number))


class Parser:
    def __init__(self):
        self.counters = {}
        self.iterations = 0
        self.warm_cache = False
        self.repository = ""

    @staticmethod
    def parse_boolean(string):
        return string == "yes" or string == "true"                          or string == "TRUE" or string == "True"

    def __parseline(self, line):
        if line[0] == "#":
            parameter = line[1:-1].replace(" ", "").split("=")
            if parameter[0] == "iterations":
                self.iterations = int(parameter[1])
            elif parameter[0] == "warm_cache":
                self.warm_cache = Parser.parse_boolean(parameter[1])
            elif parameter[0] == "repo":
                self.repository = parameter[1].split(".")[0]
        else:
            params = line.strip().split("|")
            counter_name = str(params[0])
            if len(params) == 3:
                if counter_name not in self.counters.keys():
                    counter = Counter(counter_name, params[2])
                    counter.add(params[1])
                    self.counters[counter_name] = counter
                else:
                    self.counters.get(counter_name).add(params[1])

    def parse(self, filename):
        datafile = open(filename, "r")
        for line in datafile:
            self.__parseline(line)
        datafile.close()

    def all_counters(self):
        return self.counters.values()


class Database:
    def __init__(self, credentials_file):
        file = open(credentials_file, "r")
        credentials = {}
        for line in file:
            params = line.strip().split("=")
            if len(params) == 2:
                credentials[params[0]] = params[1]
        self.user = credentials["user"]
        self.password = credentials["password"]
        self.database = credentials["database"]
        self.host = credentials["host"]
        self.port = credentials["port"]

    def write(self, repository, iterations, counters):
        client = InfluxDBClient(self.host, self.port, self.user, self.password)
        if {"name": self.database} not in client.get_list_database():
            client.create_database(self.database)
        client.switch_database(self.database)
        current_time = int(time.time())
        json_body = [{
                        "name": repository,
                        "columns": ["time"],
                        "points": []
                     }]
        for i in range(0, iterations):
            json_body[0]["points"].append([current_time])
        for counter in counters.values():
            json_body[0]["columns"].append(counter.name)
            for i in range(0, iterations):
                json_body[0]["points"][i].append(counter.numbers[i])
        client.write_points(json_body)


def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--credentials_file",
                           default="/etc/cvmfs/influxdb.credentials",
                           required=False, type=str,
                           help="Credentials file to connect to the Influx "
                                "database")
    argparser.add_argument("files", nargs="+",
                           help=".data files must be specified")
    return argparser.parse_args()


def main():
    args = parse_args()
    filenames = []
    for filename in args.files:
        for expanded in glob.glob(filename):
            filenames.append(expanded)
    parser = Parser()
    for filename in filenames:
        parser.parse(str(filename))
    database = Database(str(args.credentials_file))
    database.write(parser.repository, parser.iterations, parser.counters)
    print("Done")

if __name__ == "__main__":
    main()
