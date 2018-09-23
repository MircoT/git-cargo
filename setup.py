#!/usr/bin/env python
from setuptools import setup

with open("requirements.txt") as requirement_file:
    requirements = requirement_file.readlines()
setup(
    name='git-cargo',
    version='1.0',
    description='Python storage manager for git',
    author='Mirco Tracolli',
    author_email='m.tracolli@gmail.com',
    url='https://github.com/MircoT/git-cargo',
    packages=['cargo'],
    scripts=['bin/git-cargo'],
    install_requires=requirements,
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7'
    ],
)
