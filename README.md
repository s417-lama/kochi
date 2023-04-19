# Kochi

Kochi is a workflow management tool for compute clusters.

## Motivation

Kochi is designed to address the following challenges:
- Development of runtime systems or libraries
    - The workflow will be complicated to run benchmarks on developing runtime systems built with multiple configurations
    - Separating the runtime's git project and the benchmarking code is important, but this makes it hard to casually modify the runtime code and check the benchmark's behaviour
- Experiments with multiple machines with system-specific job managers
    - How to submit jobs to the system's job manager (e.g., slurm) depends on each system, which complicates the job execution workflow if we use multiple machines for experiments
    - Interactive job execution is sometimes impossible, but it is often the case that errors are reproduced only on a specifc machine and we want to run gdb interactively
- Version-controlled experimental results
    - Experimental results are very easy to mess up, and it is important to safely save them in a structured way
    - It often becomes untrackable which version of the code is used to produce which experimental result

## Key Features

- Unified benchmarking job specification (as a yaml file) with minimal configuration for different systems
- Flexible dependency managements for projects
- Easy application of local ad-hoc changes to the execution on remote servers
- Interactive shell execution for noninteractive system's batch job
- Gathering of experimental data produced on multiple machines (version-controlled by git)

## Machine Configuration

Kochi is designed for compute clusters of the following configuration:
```
                                                                  <Machine Name>
                                                                 ----------------
                                                           ----- | Compute Node |
                                                           |     ----------------
                                                           |
                                                           |
----------       SSH        --------------   Job manager   |     ----------------
| Client | ---------------- | Login Node | --------------------- | Compute Node |
----------                  --------------   e.g., slurm   |     ----------------
e.g., laptop                       |                       |
                                   |                       |
                                   |                       |     ----------------
                                   |                       ----- | Compute Node |
                                   |                             ----------------
                                   |     ----------------------         |
                                   ------| Shared File System |----------
                                         ----------------------
```

The local client submits *jobs* to the *workers* running on the compute servers via the login node.
The login node and compute nodes must share a file system (NFS).

Kochi can be used for a single local computer without remote login nodes or compute nodes.

## Install

You need to install Kochi to where you want to run it (including the client, login and compute nodes):
```sh
pip3 install git+https://github.com/s417-lama/kochi.git
```

## Tutorial

See https://github.com/s417-lama/kochi-tutorial
