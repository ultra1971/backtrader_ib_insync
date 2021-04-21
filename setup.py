import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="backtrader_ib_insync",
    version='0.0.1',
    description="Backtrader Interactive Brokers Store using ib_insync",
    long_description=long_description,
    license='GNU General Public License Version 3',
    url="https://github.com/ultra1971/backtrader_ib_insync",
    packages=setuptools.find_packages(),
    install_requires=['ib_insync'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3"
    ],
    python_requires='>=3.6'
)
