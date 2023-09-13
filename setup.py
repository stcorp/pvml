from setuptools import setup

exec(open("pvml/version.py").read())

setup(
    name = "pvml",
    version = __version__,
    description = "Processor Validation Management Layer",
    url = "http://github.com/stcorp/pvml",
    author = "S[&]T",
    license = "BSD",
    packages=["pvml"],
    entry_points={"console_scripts": ["pvml = pvml.__main__:main"]},
    install_requires=["lxml"],
    package_data={"pvml": ["py.typed", "xsd/*.xsd"]}
)
