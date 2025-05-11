from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="tusharemongo",
    version="0.1.0",
    author="MengtuoTie",
    author_email="your.email@example.com",
    description="将Tushare数据同步到MongoDB的工具包",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MengtuoTie/tusharemongo",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pandas",
        "pymongo",
        "tushare",
        "tqdm",
    ],
)