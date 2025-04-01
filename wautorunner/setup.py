from setuptools import setup, find_namespace_packages, Command
import os

setup(
    name='wautorunner',
    version="0.1",
    packages=find_namespace_packages(include=[
        '*',
        '*.__main__'
    ]),
    package_data={
        '': ["*"]
    },
    entry_points={
        "console_scripts": [
            "wattson-autorunner = __main__:main"
        ]
    },
    install_requires=[
        "pyyaml>=5.4",
        "numpy>=1.21.5",
    ],
    python_requires=">=3.10.0",
    author="Davide Savarro (Computer Science Department - University of Turin - UNITO)",
    author_email="davide.savarro@unito.it",
)

