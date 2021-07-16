#!/usr/bin/env python

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-mongo",
    version="2021.7.0",
    description="Dask + Mongo intergration",
    license="BSD",
    packages=find_packages(where="dask-mongo"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"test": ["pytest"]},
    include_package_data=True,
    zip_safe=False,
)
