from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(here + "/README.md", "r") as fh:
    long_description = fh.read()

with open(here + "/requirements.txt", "r") as f:
    install_requires = f.read().splitlines()

setup(
    name="ontodev-gadget",
    description="Utilities for ontology linked data tables.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="0.0.1",
    author="Rebecca Jackson",
    author_email="rbca.jackson@gmail.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: BSD License",
    ],
    install_requires=install_requires,
    packages=find_packages(),
    # entry_points={"console_scripts": ["gadget=gadget.run:main"]},
)
