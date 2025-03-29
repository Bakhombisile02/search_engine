from setuptools import setup, find_packages

setup(
    name="dlaba556_search_engine_cosc431",
    version="Alpha 0.1.0",
    description="A search engine implementation based on ISAM and B-tree structures for Information Retreval(COSC431)",
    author="Bakhombisile Siyamukela Dlamini",
    author_email="dlaba556@student.otago.ac.nz",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.20.0",
        "pytest>=6.2.5",
        "mmap3>=0.4.0",
        "cython>=0.29.24",
    ],
    python_requires=">=3.8",
)