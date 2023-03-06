from collections import namedtuple
import os
import sys
import shutil
import subprocess
import time
import datetime
import click
import tabulate

from . import util
from . import settings
from . import config
from . import project
from . import context

InstallConf = namedtuple("InstallConf", ["project", "dependency", "recipe", "on_machine", "recipe_dependencies", "context", "envs", "activate_script", "script"])
state_fields = ["project", "dependency", "recipe", "on_machine", "recipe_dependency_states", "context", "envs", "activate_script", "script", "installed_time", "commit_hash"]
State = namedtuple("State", state_fields)

def get_install_context(machine, dep, recipe):
    git_path = config.recipe_git(dep, recipe)
    if git_path:
        if not util.is_git_remote(git_path):
            with util.cwd(util.toplevel_git_dirpath()):
                with util.cwd(git_path):
                    project.sync(machine)
            return context.create_for_recipe(dep, recipe, None)
        else:
            return context.create_for_recipe(dep, recipe, git_path)
    else:
        return None

def on_complete(conf, machine, env):
    recipe_dependency_states = [get_state(conf.project, d, r, machine) for d, r in conf.recipe_dependencies.items()]
    commit_hash = subprocess.run(["git", "rev-parse", conf.context.reference], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip() if conf.context else None
    with open(settings.project_dep_install_state_filepath(conf.project, machine, conf.dependency, conf.recipe), "w") as f:
        state = State(conf.project, conf.dependency, conf.recipe, conf.on_machine, recipe_dependency_states,
                      conf.context, env, conf.activate_script, conf.script, time.time(), commit_hash)
        f.write(util.serialize(state))

def dep_env(project_name, machine, dep, recipe):
    dep_upper_name = dep.upper().replace("-", "_")
    env = dict()
    env["KOCHI_INSTALL_PREFIX_" + dep_upper_name] = settings.project_dep_install_dest_dirpath(project_name, machine, dep, recipe)
    env["KOCHI_RECIPE_" + dep_upper_name] = recipe
    return env

def deps_env(project_name, machine, deps):
    check_dependencies(project_name, machine, deps)
    dep_envs = dict()
    for d, r in deps.items():
        dep_envs.update(dep_env(project_name, machine, d, r))
    return dep_envs

def check_dependencies_aux(project_name, machine, current_dep_states, installed_dep_state):
    for ds in installed_dep_state.recipe_dependency_states:
        current_state = current_dep_states[ds.dependency]
        if current_state.installed_time != ds.installed_time:
            raise Exception(util.dedent("""
                Installation version mismatch for {0}:{1}.
                The current version of recipe {0}:{1} was installed at {2}, but {3}:{4} was installed using a version installed at {5}.
                """.format(ds.dependency, ds.recipe, datetime.datetime.fromtimestamp(current_state.installed_time),
                           installed_dep_state.dependency, installed_dep_state.recipe, datetime.datetime.fromtimestamp(ds.installed_time))))
        check_dependencies_aux(project_name, machine, current_dep_states, ds)

def check_dependencies(project_name, machine, deps):
    """
    Checks inconsistency of installation states.

    - Suppose that project A depends on project B
    - When A is installed, the installation state of B is recorded to A's state
    - If B is reinstalled, A needs to be reinstalled too
    - Such inconsistency is checked here by comparing B's current state and that recoreded in A's state
    """
    current_dep_states = {d: get_state(project_name, d, r, machine) for d, r in deps.items()}
    for d, ds in current_dep_states.items():
        print("Checking dependencies for {}:{}...".format(d, ds.recipe), flush=True)
        check_dependencies_aux(project_name, machine, current_dep_states, ds)

def install(conf, machine):
    dep_envs = deps_env(conf.project, machine, conf.recipe_dependencies)
    src_dir = settings.project_dep_install_src_dirpath(conf.project, machine, conf.dependency, conf.recipe)
    dest_dir = settings.project_dep_install_dest_dirpath(conf.project, machine, conf.dependency, conf.recipe)
    shutil.rmtree(src_dir, ignore_errors=True)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(dest_dir, exist_ok=True)
    with util.cwd(src_dir):
        with context.context(conf.context):
            with util.tee(settings.project_dep_install_log_filepath(conf.project, machine, conf.dependency, conf.recipe)) as tee:
                color = "magenta"
                where_str = "on machine {}".format(machine) if conf.on_machine else "on login node for machine {}".format(machine)
                print(click.style("Kochi installation for {}:{} started {}.".format(conf.dependency, conf.recipe, where_str), fg=color), file=tee.stdin, flush=True)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)
                env = os.environ.copy()
                env["KOCHI_MACHINE"] = machine
                env["KOCHI_INSTALL_PREFIX"] = dest_dir
                env.update(dep_envs)
                env.update(conf.envs)
                try:
                    subprocess.run("\n".join(conf.activate_script + conf.script), env=env, shell=True, executable="/bin/bash",
                                   check=True, stdout=tee.stdin, stderr=tee.stdin)
                except KeyboardInterrupt:
                    print(click.style("Kochi installation for {}:{} interrupted.".format(conf.dependency, conf.recipe), fg="red"), file=tee.stdin, flush=True)
                except BaseException as e:
                    print(click.style("Kochi installation for {}:{} failed: {}".format(conf.dependency, conf.recipe, str(e)), fg="red"), file=tee.stdin, flush=True)
                else:
                    on_complete(conf, machine, env)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)

def get_state(project_name, dependency, recipe, machine):
    try:
        with open(settings.project_dep_install_state_filepath(project_name, machine, dependency, recipe), "r") as f:
            return util.deserialize(f.read())
    except:
        raise Exception("Something went wrong with installation for dependency {}:{}. Try installing again.".format(dependency, recipe))

def show_detail(state, indent=0, **opts):
    table = []
    table.append(["Dependency Name", state.dependency])
    table.append(["Recipe Name", state.recipe])
    table.append(["Installed Time", datetime.datetime.fromtimestamp(state.installed_time)])
    table.append(["Executed on", "compute node" if state.on_machine else "login node"])
    table.append(["Context Ref", state.context.reference if state.context else None])
    table.append(["Context Commit Hash", state.commit_hash])
    table.append(["Context Diff", state.context.diff if state.context else None])
    table.append(["Environment Variables", "\n".join(["{}={}".format(k, v) for k,v in state.envs.items()])])
    table.append(["Activate Script", "\n".join(state.activate_script)])
    table.append(["Script", "\n".join(state.script)])
    print(util.dedent(tabulate.tabulate(table), indent=indent), file=opts.get("stdout", sys.stdout))
    if opts.get("recurse", False):
        indent += 2
        for d in state.recipe_dependency_states:
            print("\n", file=opts.get("stdout", sys.stdout))
            print(util.dedent("Dependency {}:{}:".format(d.dependency, d.recipe), indent=indent), file=opts.get("stdout", sys.stdout))
            show_detail(d, indent, **opts)
