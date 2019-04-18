import argparse
import json
import kazoo.client
from kazoo.exceptions import NodeExistsError, NoNodeError

JOBSERVER_DB = '/jobserver/db/{}'
MAPPING = {
    "context": "contexts",
    "contexts": "contexts",
    "job": "jobs",
    "jobs": "jobs",
    "binary": "binaries",
    "binaries": "binaries",
}
ALLOWED_FILTERS = {
    "jobs": {"statuses", "binary", "context-name", "job"},
    "contexts": {"statuses", "context-name", "context-id"},
    "binaries": {"binary"},
}
ALLOWED_STATUSES = {"RUNNING", "ERROR", "STOPPING", "FINISHED", "STARTED", "KILLED", "RESTARTING"}


class ZookeeperClient(object):

    def __init__(self, node_ips):
        if isinstance(node_ips, list):
            node_ips = ",".join(node_ips)
        self.__zk_client = kazoo.client.KazooClient(node_ips)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc_args):
        self.close()

    def connect(self):
        self.__zk_client.start()

    def close(self):
        self.__zk_client.stop()
        self.__zk_client.close()

    def get_node(self, node_path):
        self.__zk_client.sync(node_path)
        zk_tuple = self.__zk_client.get(node_path)
        return json.loads(zk_tuple[0])

    def set_node(self, node_path, value_dict):
        node_value = json.dumps(value_dict)
        try:
            self.__zk_client.set(node_path, node_value)
            print('Znode updated: "%s"' % node_path)
        except NoNodeError:
            self.__zk_client.create(node_path, node_value, makepath=True)
            print('Znode created: "%s"' % node_path)
        except Exception:
            raise Exception('Could not set value of node "%s"' % node_path)

    def get_children_list(self, node_path):
        self.__zk_client.sync(node_path)
        return self.__zk_client.get_children(node_path)

    def get_children(self, node_path):
        self.__zk_client.sync(node_path)
        return [self.get_node(node_path + "/" + path) for path in self.__zk_client.get_children(node_path)]


def extract_filters(arg_parser):
    filters = {}
    if arg_parser.filter_job:
        filters["job"] = arg_parser.filter_job
    if arg_parser.filter_context_id:
        filters["context-id"] = arg_parser.filter_context_id
    if arg_parser.filter_context_name:
        filters["context-name"] = arg_parser.filter_context_name
    if arg_parser.filter_binary:
        filters["binary"] = arg_parser.filter_binary
    if arg_parser.filter_status:
        filters["statuses"] = [status.strip() for status in arg_parser.filter_status.split(",")]
        if not set(filters["statuses"]).issubset(ALLOWED_STATUSES):
            raise Exception("Only following statuses can be used: {}".format(ALLOWED_STATUSES))
    return filters


def apply_filters(data, filters):
    for f in filters:
        if f == "statuses":
            data = [node for node in data if node["state"] in filters[f]]
        if f == "binary":
            if node_type == "binaries":
                for bin_info in data:
                    if bin_info[0]["appName"] == filters[f]:
                        data = bin_info
                        break
            else:
                data = [node for node in data if node["binaryInfo"]["appName"] == filters[f]]
        if f == "context-name":
            if node_type == "contexts":
                data = [node for node in data if node["name"] == filters[f]]
            else:
                data = [node for node in data if node["contextName"] == filters[f]]
        if f == "context-id":
            data = [node for node in data if node["id"] == filters[f]]
        if f == "job":
            if node_type == "jobs":
                data = [node for node in data if node["jobId"] == filters[f]]
    return data


def print_json(data):
    print(json.dumps(data, indent=4, sort_keys=True))


parser = argparse.ArgumentParser(description="Connect and query Jobserver's Zookeeper.")
parser.add_argument('--connection-string', type=str, dest='conn_string', required=True,
                    help='connection string for Zookeeper, e.g. localhost:2181')
parser.add_argument('--node-type', type=str, dest='node_type', required=True,
                    help='which node type to search through, e.g. context(-s), job(-s), binary(-ies)')
parser.add_argument('--status', type=str, dest='filter_status',
                    help='comma separated list of statuses to filter objects on')
parser.add_argument('--job', type=str, dest='filter_job',
                    help='jobid to filter')
parser.add_argument('--context-id', type=str, dest='filter_context_id',
                    help='context id to filter on')
parser.add_argument('--context-name', type=str, dest='filter_context_name',
                    help='context name to filter on')
parser.add_argument('--binary', type=str, dest='filter_binary',
                    help='binary name to filter on')
parser.add_argument('--only-names', action='store_true', dest='only_names',
                    help='show only names of the nodes')
parser.add_argument('--update-state', type=str, dest='update_state', help='new status for selected nodes')

args = parser.parse_args()
try:
    node_type = MAPPING[args.node_type]
except KeyError:
    raise Exception("Unknown node type {}. Allowed values: {}".format(args.node_type, MAPPING.keys()))

node_path = JOBSERVER_DB.format(node_type)
filters = extract_filters(args)
if filters and not set(filters.keys()).issubset(ALLOWED_FILTERS[node_type]):
    raise Exception("Unknown node type! Please set up --node-type flag properly. Allowed values: {}".format(MAPPING.keys()))

client = ZookeeperClient(args.conn_string)
try:
    client.connect()
    if not filters and args.only_names:
        print(client.get_children_list(node_path))
    else:
        nodes = client.get_children(node_path)
        if filters:
            nodes = apply_filters(nodes, filters)
        print_json(nodes)

        if nodes and args.update_state and node_type in ["jobs", "contexts"]:
            if not {args.update_state}.issubset(ALLOWED_STATUSES):
                raise Exception("Only following statuses can be used for update: {}".format(ALLOWED_STATUSES))
            modify = raw_input("Would you like to update status for found entities? [Yes/No]")
            if modify.lower() == "yes":
                print("Got it. Please revise updated content: ")
                for node in nodes:
                    node["state"] = args.update_state
                print_json(nodes)
                update = raw_input("Would you STILL like to update status for found entities? [Yes/No]")
                if update.lower() == "yes":
                    for node in nodes:
                        node_id = node.get("id", node.get("jobId"))
                        if node_id:
                            print("Updating node id: {}".format(node_id))
                            client.set_node(node_path + "/" + node.get("id", node.get("jobId")), node)
                        else:
                            raise Exception("Failed to find node id field (checked for id and jobId fields)")
                else:
                    print("{} is not yes. You seem unsure, will better cancel operation.".format(update))
            else:
                print("{} is not yes. Exiting:)".format(modify))
        elif args.update_state:
            print("Only jobs and contexts can be updated (with a new state).")
finally:
    client.close()
