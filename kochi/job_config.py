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

def default_name(path, machine):
    return dict_get(root_config(path), "default_name", default=None)

def default_queue(path, machine):
    return dict_get(root_config(path), "default_queue", default=None)

def default_duplicates(path, machine):
    return dict_get(root_config(path), "default_duplicates", default=1)

def parse_dependencies(dep_conf, machine):
    deps = collections.OrderedDict()
    for dep_dict in dep_conf:
        if not "machines" in dep_dict or machine in dep_dict["machines"]:
            deps[dep_dict["name"]] = dep_dict["recipe"]
    return deps

def default_dependencies(path, machine):
    return parse_dependencies(dict_get(root_config(path), "depends", default=[]), machine)

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
    build_conf = dict_get(root_config(path), "build", default=dict())
    build_conf.update(script=wrap_list(dict_get(build_conf, "script", default=[])))
    return build_conf

# Run
# -----------------------------------------------------------------------------

def run(path):
    run_conf = dict_get(root_config(path), "run", default=dict())
    run_conf.update(script=wrap_list(dict_get(run_conf, "script", default=[])))
    return run_conf

# Batches
# -----------------------------------------------------------------------------

def batches(path):
    return dict_get(root_config(path), "batches")

def batch(path, batch_name):
    return dict_get(batches(path), batch_name)

def batch_job_name(path, batch_name, machine):
    return dict_get(batch(path, batch_name), "name", default=None) or default_name(path, machine)

def batch_queue(path, batch_name, machine):
    return dict_get(batch(path, batch_name), "queue", default=None) or default_queue(path, machine)

def batch_duplicates(path, batch_name, machine):
    return dict_get(batch(path, batch_name), "duplicates", default=None) or default_duplicates(path, machine)

def batch_params(path, batch_name, machine):
    params = default_params(path, machine)
    params.update(parse_params(dict_get(batch(path, batch_name), "params", default=dict()), machine))
    return params

def batch_dependencies(path, batch_name, machine):
    deps = default_dependencies(path, machine)
    deps.update(parse_dependencies(dict_get(batch(path, batch_name), "depends", default=dict()), machine))
    return deps

def batch_build(path, batch_name, machine):
    build_conf = build(path)
    build_conf.update(dict_get(batch(path, batch_name), "build", default=dict()))
    build_conf.update(script=wrap_list(dict_get(build_conf, "script", default=[])))
    return build_conf

def batch_run(path, batch_name, machine):
    run_conf = run(path)
    run_conf.update(dict_get(batch(path, batch_name), "run", default=dict()))
    run_conf.update(script=wrap_list(dict_get(run_conf, "script", default=[])))
    return run_conf

def batch_artifacts(path, batch_name, machine):
    return dict_get(batch(path, batch_name), "artifacts", default=[])
