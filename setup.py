#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-mongo",
    version="2022.5.0",
    description="Dask + Mongo intergration",
    license="BSD",
    packages=["dask_mongo"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"test": ["pytest"]},
    include_package_data=True,
    zip_safe=False,
)
