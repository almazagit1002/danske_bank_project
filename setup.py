from setuptools import setup, find_packages

setup(
    name="fraud-data-pipeline",
    version="0.1.0",
    description="End-to-end data engineering pipeline for fraud analytics using a medallion architecture",
    author="Your Name",
    packages=find_packages(exclude=("tests", "notebooks")),
    install_requires=[
        line.strip()
        for line in open("requirements.txt")
        if line.strip() and not line.startswith("#")
    ],
    python_requires=">=3.9",
)
