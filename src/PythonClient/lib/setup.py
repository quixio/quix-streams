import setuptools
from setuptools_cythonize import get_cmdclass

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="quixstreaming",
    version="0.1.0",
    author="Quix Developers",
    author_email="developers@quix.ai",
    description="Package used to send messages to Quix platform",
    long_description=long_description,
    cmdclass=get_cmdclass(),
    long_description_content_type="text/markdown",
    url="https://quix.ai",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[        
        "Development Status :: 2 - Pre-Alpha"
        "Intended Audience :: Developers",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X"
        "Programming Language :: Python :: 3"        
    ],
    python_requires='>=3.6, <3.9',
    install_requires=[
        'pythonnet==2.5.1',
        'pandas>=1.0.0,<2',
        'Deprecated>=1.1,<2',
        'pytz>=2020.1',
        'tzlocal>=3.0',
        'setuptools-cythonize>=1.0.6,<2'
    ]
)