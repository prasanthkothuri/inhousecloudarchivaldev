from setuptools import setup, find_packages

setup(
    name="offloadlib",
    version="0.1.0",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=6.0"
    ],
)
