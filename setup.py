from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="FaaSr_py",
    version="0.1.13",
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
)
