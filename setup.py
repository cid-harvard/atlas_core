import os
from setuptools import setup


def read(fname, lines=False):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    if lines:
        return [x.strip() for x in f.readlines()]
    else:
        return f.read()

setup(
    name="atlas_core",
    version="0.1",
    author="Mali Akmanalp <Harvard CID>",
    description=("Core building blocks for atlas projects at CID."),
    url="http://github.com/cid-harvard/atlas_core",
    packages=['atlas_core'],
    install_requires=read("requirements.txt", lines=True),
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
