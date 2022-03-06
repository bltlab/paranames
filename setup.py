#! /usr/bin/env python

from os import path

from setuptools import find_packages, setup


def setup_package() -> None:
    root = path.abspath(path.dirname(__file__))
    with open(path.join(root, "README.md"), encoding="utf-8") as f:
        long_description = f.read()

    setup(
        name="paranames",
        version="0.0.1",
        packages=find_packages(include=("paranames", "paranames.*")),
        # Package type information
        package_data={"paranames": ["py.typed"]},
        # Python 3.8
        python_requires="==3.8.*",
        install_requires=[
            "attrs",
            "black",
            "click",
            "csvkit",
            "dask",
            "dictances",
            "editdistance",
            "flake8",
            "mypy",
            "numpy",
            "orjson",
            "pandas",
            "pyicu",
            "pymongo",
            "qwikidata",
            "requests",
            "rich",
            "scipy",
            "sklearn",
            "tqdm",
            "unicodeblock"
        ],
        license="MIT",
        description="ParaNames",
        long_description=long_description,
        classifiers=[
            "Development Status :: 2 - Pre-Alpha",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Topic :: Scientific/Engineering :: Artificial Intelligence",
        ],
        url="https://github.com/bltlab/paranames",
        long_description_content_type="text/markdown",
        author="Jonne Saleva",
        author_email="jonnesaleva@brandeis.edu",
    )


if __name__ == "__main__":
    setup_package()
