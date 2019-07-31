from setuptools import setup, find_packages

version = "0.1.0"
setup(
    name="gmaildump",
    version=version,
    description="A tool to get the data from gmail and store it in database",
    keywords="gmaildump",
    install_requires=[
        "pymongo==2.7.2",
        "tornado==4.5.3",
        "basescript==0.2.0",
        "deeputil==0.2.5",
        "gnsq==0.4.0",
    ],
    package_dir={"gmaildump": "gmaildump"},
    packages=find_packages("."),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
    ],
    test_suite="test.suitefn",
    entry_points={"console_scripts": ["gmaildump = gmaildump:main"]},
)
