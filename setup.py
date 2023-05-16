from setuptools import setup, find_namespace_packages, Command
import os
from install.install_command import WattsonInstallDependencies


class WattsonRequirementsInstall(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        path = os.path.dirname(os.path.realpath(__file__))
        print("Installing Wattson Dependencies for Ubuntu via setup.py")
        os.system(f"sh {path}/ubuntu/MAIN.sh {path}/ubuntu/tmp")


setup(
    name='wattson',
    version="1.0.0",
    packages=find_namespace_packages(include=[
        'wattson.*',
        'wattson.__main__'
    ]),
    package_data={
        '': ["*"]
    },
    entry_points={
        "console_scripts": [
            "wattson = wattson.topology.__main__:main",
            "wattson-dedicated = wattson.topology.__main__:standalone",
            "wattson-stats = wattson.analysis.statistics.__main__:main"
            "wattson-preview = wattson.analysis.preview.__main__:main"
            "wattson-clean = wattson.util.clean.__main__:main"
        ]
    },
    cmdclass={
        "ubuntu": WattsonRequirementsInstall,
        "wattson": WattsonInstallDependencies,
    },
    install_requires=[
        'wheel',
        'ifcfg',
        'ninja',
        'c104==1.16.0',
        'testresources',
        'python-dateutil',
        'docker',
        'ipaddress==1.0.23',
        'matplotlib>=3.1.2',
        'more_itertools==5.0.0',
        'netifaces==0.11.0',
        'networkx>=2.5',
        'numba>=0.55.2',
        'numpy>=1.21.0',
        'packaging==20.3',
        'pandapower==2.10.1',
        'pandas==1.3.4',
        'psutil==5.7.0',
        'pygraphviz==1.9',
        'pydot',
        'pymodbus==2.5.2',
        'pytest',
        'igraph==0.9.9',
        'python-iptables@git+https://github.com/lennart-bader/python-iptables.git',
        'qtpy',
        'PyQt5==5.15.4',
        'PyQtWebEngine==5.15.4',
        'pywebview>=3.3.5',
        'pyyaml==5.3.1',
        'pyzmq==20.0.0',
        'scapy==2.4.4',
        'scipy==1.8.1',
        'setuptools==52.0.0',
        'shapely',
        'sqlalchemy==1.3.16',
        'tabulate',
        'twisted==21.7.0',
        'ifcfg'
    ],
    python_requires=">=3.10.0",
    author="Lennart Bader (Fraunhofer FKIE)",
    author_email="lennart.bader@fkie.fraunhofer.de",
)

