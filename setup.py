from setuptools import find_packages, setup

setup(
    name="bfi_dagster_project",
    packages=find_packages(exclude=["processing_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "tenacity",
        "requests",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

