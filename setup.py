import os
from setuptools import setup, find_packages


def read(fname, lines=False):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    if lines:
        return [x.strip() for x in f.readlines()]
    else:
        return f.read()


setup(
    name="atlas_core",
    version="v0.2.7",
    author="Mali Akmanalp <Harvard CID>",
    description=("Core building blocks for atlas projects at CID."),
    url="http://github.com/cid-harvard/atlas_core",
    packages=find_packages(exclude=["atlas_core.sample"]),
    install_requires=[
        "Flask>=0.12.2,<1",
        "SQLAlchemy>=1.1.14,<2",
        "flask-sqlalchemy>=2.0,<3",
        "toastedmarshmallow==0.2.6",
        "lima>=0.5,<1",
        "six>=1.0.0,<2",
    ],
    long_description=read("README.md"),
    classifiers=[
        "Framework :: Flask",
        "Environment :: Web Environment",
        "Programming Language :: Python :: 3.4",
        "Development Status :: 3 - Alpha",
        "Topic :: Internet",
        "License :: OSI Approved :: BSD License",
    ],
)
