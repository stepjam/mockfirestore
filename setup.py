# Write the setup file for the mockfirestore package

from setuptools import find_packages, setup

setup(
    name="mockfirestore",
    version="0.1",
    packages=find_packages(),
    install_requires=[],
    author="Stephen James",
    description="A mock Firestore package for testing purposes",
)
