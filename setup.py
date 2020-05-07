import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark-me", # Replace with your own username
    version="0.0.4",
    author="Ivan Georgiev",
    #author_email="ivan.georgiev",
    description="Pyspark tools for everyday use",
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
    python_requires='>=3.6',
)
