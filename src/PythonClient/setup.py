import platform
import setuptools
import os
import os.path
import re
import fileinput

package_version = "0.5.6.dev4"

with open("README.md", "r") as fh:
    long_description = fh.read()

def get_data_files():
    licenses = [file for file in os.listdir('.') if file.startswith('LICENSE')]
    if os.path.isfile('../../LICENSE'):
        licenses.append('../../LICENSE')  # non-docker build
    else:
        licenses.append('../LICENSE')  # docker build
    return [('licences', licenses)]


QUIXSTREAMS_PACKAGE_DATA = []
def rec_incl_with_filter(directory, filters, trim):
    trimfirstn=len(trim) + 1
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            for filter in filters:
                match = re.match(filter, filename)
                if match is not None:
                    filepath = os.path.join(path, filename)[trimfirstn:]
                    #print("F: {}".format(filepath))
                    QUIXSTREAMS_PACKAGE_DATA.append(filepath)
                    break


rec_incl_with_filter('src/quixstreams', [r'.*\.py$'], 'src/quixstreams')  # include all python files
plat = platform.uname()
platname = f"{plat.system}-{plat.machine}".lower()  # If changes, update build scripts and __init__.py
platpath = 'src/quixstreams/native/' + platname
print("Platform {} build".format(plat))
if plat.system.upper() == "WINDOWS":
    rec_incl_with_filter(platpath, [r'.*\.dll$'], 'src/quixstreams')
elif plat.system.upper() == "DARWIN":
    rec_incl_with_filter(platpath, [r'.*\.dylib$'], 'src/quixstreams')
elif plat.system.upper() == "LINUX":
    rec_incl_with_filter(platpath, [r'.*\.so$'], 'src/quixstreams')
else:
    raise Exception("Not supported build platform {}".format(platname))

def update_version_in_package(vers: str, fromvers: str = "local"):
    for line in fileinput.input("src/quixstreams/__init__.py", inplace=True):
        if line.startswith("__version__ = "):
            line = line.replace(fromvers, vers)
        print(line, end='')

update_version_in_package(package_version)
try:
    setuptools.setup(
        name="quixstreams",
        version=package_version,
        author="Quix Analytics Ltd",
        author_email="devs@quix.io",
        description="Library optimized for easily sending and reading time series data using Kafka",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/quixio/quix-streams",
        packages=setuptools.find_packages('src'),
        package_data={
            'quixstreams': QUIXSTREAMS_PACKAGE_DATA
        },
        license="Apache 2.0",
        package_dir={'': 'src'},
        data_files=get_data_files(),
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: Microsoft :: Windows",
            "Operating System :: POSIX :: Linux",
            "Operating System :: MacOS :: MacOS X",
            "Programming Language :: Python :: 3"
        ],
        python_requires='>=3.6, <4',
        install_requires=[
            'pandas>=1.0.0,<2',
            'Deprecated>=1.1,<2'
        ]
    )
finally:
    update_version_in_package("local", package_version)