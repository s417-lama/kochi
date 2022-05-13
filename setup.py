from setuptools import setup, find_packages

import kochi.settings

kochi.settings.init()

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
    ],
)