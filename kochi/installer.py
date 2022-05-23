from collections import namedtuple
import os
import subprocess
import sys
import time
import datetime
import click
import tabulate

from . import util
from . import settings
from . import project
from . import context

InstallConf = namedtuple("InstallConf", ["project", "dependency", "recipe", "context", "envs", "commands"])
state_fields = ["project", "dependency", "recipe", "context", "envs", "commands", "installed_time", "commit_hash"]
State = namedtuple("State", state_fields)

def get_install_context(dep_config, recipe_config, login_host, git_remote):
    if dep_config.get("local_path") and dep_config.get("git_remote"):
        print("'local_path' and 'git_remote' cannot be simultaneously specified in dependency config.", file=sys.stderr)
        exit(1)
    if dep_config.get("local_path"):
        local_dep_path = os.path.join(util.toplevel_git_dirpath(), dep_config["local_path"])
        with util.cwd(local_dep_path):
            if not git_remote:
                project.sync(login_host)
            return context.create_with_project_config(recipe_config, git_remote)
    elif dep_config.get("git_remote"):
        if recipe_config.get("current_status"):
            print("'current_status' cannot be used in 'git_remote' dependency.", file=sys.stderr)
            exit(1)
        return context.create_with_project_config(recipe_config, dep_config["git_remote"])
    else:
        return None

def on_complete(conf, machine):
    commit_hash = subprocess.run(["git", "rev-parse", conf.context.reference], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip() if conf.context else None
    with open(settings.project_dep_install_state_filepath(conf.project, machine, conf.dependency, conf.recipe), "w") as f:
        state = State(conf.project, conf.dependency, conf.recipe, conf.context, conf.envs, conf.commands, time.time(), commit_hash)
        f.write(util.serialize(state))

def install(conf, machine):
    prefix = settings.project_dep_install_dirpath(conf.project, machine, conf.dependency, conf.recipe)
    os.makedirs(prefix, exist_ok=True)
    with util.tmpdir(settings.project_dep_install_tmp_dirpath(conf.project, machine, conf.dependency, conf.recipe)):
        with context.context(conf.context):
            with util.tee(settings.project_dep_install_log_filepath(conf.project, machine, conf.dependency, conf.recipe)) as tee:
                color = "magenta"
                print(click.style("Kochi installation for {}:{} started on machine {}.".format(conf.dependency, conf.recipe, machine), fg=color), file=tee.stdin, flush=True)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)
                env = os.environ.copy()
                env["KOCHI_MACHINE"] = machine
                env["KOCHI_INSTALL_PREFIX"] = prefix
                for k, v in conf.envs.items():
                    env[k] = v
                try:
                    subprocess.run(util.decorate_command(conf.commands), env=env, shell=True, check=True, stdout=tee.stdin, stderr=tee.stdin)
                except KeyboardInterrupt:
                    print(click.style("Kochi installation for {}:{} interrupted.".format(conf.dependency, conf.recipe), fg="red"), file=tee.stdin, flush=True)
                except BaseException as e:
                    print(click.style("Kochi installation for {}:{} failed: {}".format(conf.dependency, conf.recipe, str(e)), fg="red"), file=tee.stdin, flush=True)
                else:
                    on_complete(conf, machine)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)

def get_state(project_name, dependency, recipe, machine):
    try:
        with open(settings.project_dep_install_state_filepath(project_name, machine, dependency, recipe), "r") as f:
            return util.deserialize(f.read())
    except:
        raise Exception("Something went wrong with installation for dependency {}:{}. Try installing again.".format(dependency, recipe))

def show_detail(state):
    table = []
    table.append(["Dependency Name", state.dependency])
    table.append(["Recipe Name", state.recipe])
    table.append(["Installed Time", datetime.datetime.fromtimestamp(state.installed_time)])
    table.append(["Context Ref", state.context.reference if state.context else None])
    table.append(["Context Commit Hash", state.commit_hash])
    table.append(["Context Diff", state.context.diff if state.context else None])
    table.append(["Environments", state.envs])
    table.append(["Commands", state.commands])
    print(tabulate.tabulate(table))
