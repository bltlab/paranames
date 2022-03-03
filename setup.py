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
        # 3.7 and up, but not Python 4
        python_requires="~=3.7",
        license="MIT",
        description="ParaNames",
        long_description=long_description,
        # install_requires=[
        # "attrs>=19.2.0",
        # "click",
        # "tabulate",
        # ],
        # entry_points="""
        # [console_scripts]
        # paranames=paranames.scripts.paranames:cli
        # """,
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
