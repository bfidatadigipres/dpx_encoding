from setuptools import find_packages, setup


if __name__ == "__main__":
    setup(
        name="bfi_rawcooked_dagster",
        packages=find_packages(exclude=["bfi_rawcooked_tests"]),
        install_requires=[
            "dagster",
            "requests"
        ],
    )
