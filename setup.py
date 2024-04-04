# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# This call to setup() does all the work
setup(
    name="delab-socialmedia",
    version="0.3.6",
    description="a library to download reply trees in forums and social media",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/juliandehne/delab-socialmedia",
    author="Julian Dehne",
    author_email="julian.dehne@gmail.com",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent"
    ],
    packages=find_packages(),
    include_package_data=True,
    install_requires=["delab_trees", "Mastodon.py", "twarc==2.8.0", "PyYAML", "pytest==7.1.2", "attrs", "python-dotenv"]
)
