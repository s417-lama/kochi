import sys
import yaml

from . import settings

loaded_config = None

def root_config():
    global loaded_config
    if not loaded_config:
        try:
            with open(settings.config_filepath(), "r") as f:
                loaded_config = yaml.safe_load(f)
        except FileNotFoundError:
            print("Config file was not found at {}.".format(settings.config_filepath()), file=sys.stderr)
            exit(1)
        except yaml.YAMLError as e:
            raise Exception("Error while loading config file {}:".format(settings.config_filepath())) from e
    return loaded_config

def keyerror(key):
    print("Config does not have key '{}' (config file: {})".format(key, settings.config_filepath()), file=sys.stderr)
    exit(1)

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

# Machine
# -----------------------------------------------------------------------------

def machines():
    return dict_get(root_config(), "machines")

def machine_list():
    return [k for k in machines()]

def machine(m):
    return dict_get(machines(), m)

def login_host(m):
    return dict_get(machine(m), "login_host")

def alloc_script(m):
    return wrap_list(dict_get(machine(m), "alloc_script"))

def alloc_interact_script(m):
    return wrap_list(dict_get(machine(m), "alloc_interact_script"))

def load_env_script(m):
    return wrap_list(dict_get(machine(m), "load_env_script", default=[]))

def work_dir(m):
    return dict_get(machine(m), "work_dir", default=None)

# Dependencies
# -----------------------------------------------------------------------------

def dependencies():
    return dict_get(root_config(), "dependencies")

def dependency_list():
    return [k for k in dependencies()]

def dependency(d):
    return dict_get(dependencies(), d)

def recipes(d):
    return dict_get(dependency(d), "recipes")

def recipe_list(d):
    return [k for k in recipes(d)]

def recipe(d, r):
    return dict_get(recipes(d), r)

def recipe_envs(d, r):
    envs = dict()
    envs.update(dict_get(dependency(d), "envs", default=dict()))
    envs.update(dict_get(recipe(d, r), "envs", default=dict()))
    return envs

def recipe_script(d, r):
    cmds = wrap_list(dict_get(recipe(d, r), "before_script", default=None) or dict_get(dependency(d), "before_script", default=[])) + \
           wrap_list(dict_get(recipe(d, r), "script"       , default=None) or dict_get(dependency(d), "script"       , default=[])) + \
           wrap_list(dict_get(recipe(d, r), "after_script" , default=None) or dict_get(dependency(d), "after_script" , default=[]))
    if len(cmds) == 0:
        print("Recipe {}:{} does not have installation scripts (config file: {})".format(d, r, settings.config_filepath()), file=sys.stderr)
        exit(1)
    else:
        return cmds

def recipe_git(d, r):
    return dict_get(recipe(d, r), "git", default=None) or dict_get(dependency(d), "git", default=None)

def recipe_current_state(d, r):
    return dict_get(recipe(d, r), "current_state", default=None) or dict_get(dependency(d), "current_state", default=False)

def recipe_branch(d, r):
    return dict_get(recipe(d, r), "branch", default=None) or dict_get(dependency(d), "branch", default=None)

def recipe_commit_hash(d, r):
    return dict_get(recipe(d, r), "commit_hash", default=None) or dict_get(dependency(d), "commit_hash", default=None)
