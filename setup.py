from setuptools import setup, find_namespace_packages
import os
from install.install_command import WattsonInstallDependencies


setup(
    name='wattson',
    version="2.0.3",
    packages=find_namespace_packages(include=[
        'wattson.*',
        'wattson.__main__'
    ]),
    package_data={
        '': ["*"]
    },
    cmdclass={
        "wattson": WattsonInstallDependencies,
    },
    install_requires=[
        'wheel',
        'ifcfg',
        'testresources',
        # Current c104 master is bugged.
        'c104==2.0.1',
        'python-dateutil',
        'docker',
        "drawsvg",
        "flask",
        'ipaddress>=1.0.23',
        'matplotlib>=3.1.2',
        'more_itertools>=5.0.0',
        'netifaces>=0.11.0',
        'networkx>=2.5',
        'numba>=0.57.1',
        'numpy',
        'pandapower>=3.0.0',
        'pandas>=1.3.4',
        'psutil>=5.7.0',
        'pydot',
        'pymodbus',
        'pyprctl',
        'pytest',
        'igraph>=0.9.9',
        'python-iptables@git+https://github.com/lennart-bader/python-iptables.git',
        'pathfinding',
        'pyprctl',
        'qtpy',
        'pydantic',
        'pytimeparse2>=1.6.0',
        'PyQt6>=6.5.2',
        'PySide6',
        'PyQt6-WebEngine>=6.5.0',
        'pywebview>=3.3.5',
        'pyyaml>=5.4',
        'pyzmq>=20.0.0',
        'scapy>=2.4.4',
        #'scipy>=1.14.1',
        'scipy',
        'setuptools>=65.5.1',
        'shapely',
        'sqlalchemy>=1.3.16',
        'tabulate',
        'twisted>=22.2.0',
        "unidecode",
        'ifcfg'
    ],
    python_requires=">=3.12.0",
    author="Lennart Bader (Fraunhofer FKIE)",
    author_email="lennart.bader@fkie.fraunhofer.de",
)

