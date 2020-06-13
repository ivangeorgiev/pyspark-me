import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark-me",
    version="0.0.6",
    author="Ivan Georgiev",
    #author_email="ivan.georgiev",
    description="Databricks client SDK with command line client for Databricks REST APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ivangeorgiev/pyspark-me",
    packages=setuptools.find_packages(),
    install_requires=[
        'click',
        'requests'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points='''
        [console_scripts]
        dbr-me=pysparkme.databricks.cli:cli
    ''',
    python_requires='>=3.6',
)
