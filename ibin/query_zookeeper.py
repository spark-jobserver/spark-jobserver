import argparse
import json
import logging
import kazoo.client
from kazoo.exceptions import NoNodeError

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
    "jobs": {"statuses", "binary", "context-name", "job", "context-id"},
    "contexts": {"statuses", "context-name", "context-id"},
    "binaries": {"binary"},
}
ALLOWED_STATUSES = {"RUNNING", "ERROR", "STOPPING", "FINISHED", "STARTED", "KILLED", "RESTARTING"}
PROHIBITED_FOR_UPDATE = {"id", "jobId", "contextName", "name"}
NODE_TYPES_TO_CHANGE = {"contexts", "jobs"}


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

    def delete_node(self, node_path):
        try:
            self.__zk_client.delete(node_path, recursive=True)
        except NoNodeError:
            logging.error("Couldn't delete node %s: node doesn't exist" % node_path)

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
        filter_value = filters[f]
        if f == "statuses":
            data = [node for node in data if node["state"] in filter_value]
        if f == "binary":
            if node_type == "binaries":
                found = False
                result = []
                for bin_info in data:
                    if bin_info[0]["appName"] == filter_value:
                        result.append(bin_info)
                        found = True
                data = result if found else []
            else:
                data = [node for node in data if node["binaryInfo"]["appName"] == filter_value]
        if f == "context-name":
            if node_type == "contexts":
                data = [node for node in data if node["name"] == filter_value]
            else:
                data = [node for node in data if node["contextName"] == filter_value]
        if f == "context-id":
            if node_type == "contexts":
                data = [node for node in data if node["id"] == filter_value]
            elif node_type == "jobs":
                data = [node for node in data if node["contextId"] == filter_value]
        if f == "job":
            if node_type == "jobs":
                data = [node for node in data if node["jobId"] == filter_value]
    return data


def print_json(data, only_names=False):
    if only_names:
        if node_type == "binaries":
            print([d[0]["appName"] for d in data])  # each node is a list of binaries, e.g. [{..}, .. ,{..}]
        elif node_type == "contexts":
            print([d["id"] for d in data])
        elif node_type == "jobs":
            print([d["jobId"] for d in data])
    else:
        print(json.dumps(data, indent=4, sort_keys=True))


def get_user_confirmation(question_text):
    user_answer = raw_input(question_text).lower()
    return user_answer == "y" or user_answer == "yes"


def delete_nodes(nodes):
    if nodes and get_user_confirmation("Are you sure you want to delete all selected {} node(s)?".format(len(nodes))):
        for node in nodes:
            if node_type == "binaries":
                full_node_path = node_path + "/" + node[0]["appName"]
            else:
                full_node_path = node_path + "/" + node.get("id", node.get("jobId"))
            client.delete_node(full_node_path)


def update_nodes(nodes, field_to_update, new_value):
    if nodes and field_to_update and new_value:
        if node_type not in NODE_TYPES_TO_CHANGE:
            raise Exception("Only node types {} are allowed to be updated.".format(NODE_TYPES_TO_CHANGE))
        if field_to_update not in nodes[0].keys():
            raise Exception("Selected node type has only following fields: {}".format(nodes[0].keys()))
        if field_to_update == "state" and not {new_value}.issubset(ALLOWED_STATUSES):
            raise Exception("Only following states can be used for update: {}".format(ALLOWED_STATUSES))
        if field_to_update in PROHIBITED_FOR_UPDATE:
            raise Exception("Following fields are not allowed to be changed: {}".format(PROHIBITED_FOR_UPDATE))
        if get_user_confirmation("Would you like to update status for found entities? [Yes/No]"):
            print("Got it. Please revise updated content: ")
            for n in nodes:
                n[field_to_update] = new_value
            print_json(nodes)
            if get_user_confirmation("Would you STILL like to update {} for found entities? [Yes/No]".format(
                    field_to_update
            )):
                for node in nodes:
                    node_id = node.get("id", node.get("jobId"))
                    if node_id:
                        print("Updating node id: {}".format(node_id))
                        client.set_node(node_path + "/" + node.get("id", node.get("jobId")), node)
                    else:
                        raise Exception("Failed to find node id field (checked for id and jobId fields)")
            else:
                print("Didn't get confirmation. Aborting operation.")
        else:
            print("Didn't get confirmation. Aborting operation.")
    elif field_to_update and not new_value:
        print("No new value for the update field was given.")
    elif new_value and not field_to_update:
        print("Please specify which field to update with new value.")


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
parser.add_argument('--update-field', dest='update_field',
                    help='field to update, should be used together with --update-value')
parser.add_argument('--update-value', dest='update_value',
                    help='new value for selected field')
parser.add_argument('--update-end-time-string', type=str, dest='update_end_time',
                    help='new end time for selected nodes')
parser.add_argument('--limit', type=int, dest='limit', help='limit number of results')
parser.add_argument('--delete', action='store_true', dest='delete', help='delete selected nodes')
parser.add_argument('--sort-by', type=str, dest='sort_by', help='name of the field to sort by. Not for binaries!')

args = parser.parse_args()
try:
    node_type = MAPPING[args.node_type]
except KeyError:
    raise Exception("Unknown node type {}. Allowed values: {}".format(args.node_type, MAPPING.keys()))

node_path = JOBSERVER_DB.format(node_type)
filters = extract_filters(args)
sort_by = args.sort_by

if filters and not set(filters.keys()).issubset(ALLOWED_FILTERS.get(node_type, [])):
    raise Exception("Filter is not allowed for this node-type! Allowed values: {}".format(
        ALLOWED_FILTERS.get(node_type, []))
    )

client = ZookeeperClient(args.conn_string)
try:
    client.connect()
    nodes = []
    try:
        nodes = client.get_children(node_path)
    except kazoo.exceptions.NoNodeError:
        print("No nodes of this type found")
    if filters:
        nodes = apply_filters(nodes, filters)
    if nodes:
        if sort_by:
            if node_type == "binaries":
                raise Exception("Can't sort binaries, sorry.")
            elif sort_by not in nodes[0].keys():
                raise Exception("Selected nodes can be sorted only by following fields: {}".format(nodes[0].keys()))
            else:
                nodes = sorted(nodes, key=lambda x: x[sort_by])
        if args.limit:
            nodes = nodes[:args.limit]
    print_json(nodes, args.only_names)
    update_nodes(nodes, args.update_field, args.update_value)
    if args.delete:
        delete_nodes(nodes)
finally:
    client.close()
