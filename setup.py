from setuptools import find_packages, setup

setup(
    name="pyawstools",
    version="0.0.1",
    author="Nils Naumann",
    packages=find_packages(),
    install_requires=[
        "boto3==1.35.5",
    ],
    package_data={"pyawstools": []},
)
