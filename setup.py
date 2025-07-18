from setuptools import find_packages, setup

setup(
    name="ageing-report-pipeline",
    version="1.0.0",
    description="PySpark pipeline for generating ageing reports",
    author="Data Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "test": ["pytest>=7.0.0", "pytest-cov>=4.0.0"],
    },
    entry_points={
        "console_scripts": [
            "ageing-report=main:main",
        ],
    },
)
