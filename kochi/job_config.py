import sys
import collections
import yaml

loaded_config = dict()

def root_config(path):
    global loaded_config
    if not path in loaded_config:
        try:
            with open(path, "r") as f:
                loaded_config[path] = yaml.safe_load(f)
        except FileNotFoundError:
            print("Job config file was not found at {}.".format(path), file=sys.stderr)
            exit(1)
        except yaml.YAMLError as e:
            raise Exception("Error while loading job config file {}:".format(path)) from e
    return loaded_config[path]

def keyerror(key):
    raise Exception("Job config does not have key '{}'".format(key))

def dict_get(d, k, **opts):
    if "default" in opts:
        return d.get(k, opts["default"])
    else:
        try:
            return d[k]
        except:
            keyerror(k)

def wrap_list(s):
    return s if isinstance(s, list) else [s]

# Defaults
# -----------------------------------------------------------------------------

def default_queue(path, machine):
    return dict_get(root_config(path), "default_queue", default=None)

def default_dependencies(path, machine):
    dep_conf = dict_get(root_config(path), "depends", default=[])
    deps = collections.OrderedDict()
    for dep_dict in dep_conf:
        if not "machines" in dep_dict or machine in dep_dict["machines"]:
            deps[dep_dict["name"]] = dep_dict["recipe"]
    return deps

# Parameters
# -----------------------------------------------------------------------------

def parse_params(params_conf, machine, allow_multi=True):
    params = dict()
    for param_name, param_conf in params_conf.items():
        if isinstance(param_conf, list):
            if len(param_conf) > 0 and isinstance(param_conf[0], dict):
                # list of dict to be filtered by machies
                for pc in param_conf:
                    if not "machines" in pc or machine in pc["machines"]:
                        v = pc["value"]
            else:
                # list of values
                v = param_conf
        else:
            # a value
            v = param_conf
        if isinstance(v, list) and not allow_multi:
            print("Parameter '{}' cannot have multiple default values ({})".format(param_name, v), file=sys.stderr)
            exit(1)
        params[param_name] = v
    return params

def default_params(path, machine):
    return parse_params(dict_get(root_config(path), "default_params", default=dict()), machine, False)

# Build
# -----------------------------------------------------------------------------

def build(path):
    return dict_get(root_config(path), "build")

def build_script(path, machine):
    return wrap_list(dict_get(build(path), "script"))

# Run
# -----------------------------------------------------------------------------

def run(path):
    return dict_get(root_config(path), "run")

def run_script(path, machine):
    return wrap_list(dict_get(run(path), "script"))
