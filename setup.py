from setuptools import setup, find_packages

setup(
    name="kochi",
    version="0.0.1",
    author="Shumpei Shiina",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "kochi = kochi.cli:cli",
        ],
    },
    install_requires=[
        "click",
        "pyyaml",
        "psutil",
        "tabulate",
        "pexpect",
        "graphlib-backport", # not needed for Python >= 3.9
    ],
)
