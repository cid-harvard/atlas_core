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
    version="v0.2.2",
    author="Mali Akmanalp <Harvard CID>",
    description=("Core building blocks for atlas projects at CID."),
    url="http://github.com/cid-harvard/atlas_core",
    packages=find_packages(exclude=["atlas_core.sample"]),
    install_requires=[
        'Flask>=0.10.1,<1',
        'Flask-Babel>=0.9,<1',
        'SQLAlchemy>=0.9.8,<1',
        'flask-sqlalchemy>=2.0,<3',
        'flask-script>=2.0.5,<2',
        'marshmallow>=1.2.2,<2,'
    ],
    long_description=read('README.md'),
    classifiers=[
        "Framework :: Flask",
        "Environment :: Web Environment",
        "Programming Language :: Python :: 3.4",
        "Development Status :: 3 - Alpha",
        "Topic :: Internet",
        "License :: OSI Approved :: BSD License",
    ],
)
