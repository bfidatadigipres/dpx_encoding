from setuptools import find_packages, setup

setup(
    name="dagster_rawcooked",
    packages=find_packages(exclude=["dagster_rawcooked_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
