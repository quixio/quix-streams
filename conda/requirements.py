import re
import sys
from pathlib import Path

PYPI_OPTIONAL_GROUP = re.compile(r"all\s*=\s*\[\n(.*?)\n\]", re.DOTALL)
PYPI_MAIN_REQUIREMENTS = re.compile(r"([\w\[\],-]+)([><=,.\d]*)?")
PYPI_OPTIONAL_REQUIREMENTS = re.compile(r"([\w\[\],-]+)([><=,.\d]*)\",?")

CONDA_META_GROUP = re.compile(r"run:\n(.+)\ntest", re.DOTALL)
CONDA_META_REQUIREMENTS = re.compile(r"\s{4}- ([\w-]+) ([><=,.\d]*)?")
CONDA_POST_LINK_REQUIREMENTS = re.compile(r"'([\w\[\],-]+)([><=,.\d]*)?")

PYPI_TO_CONDA_NAME_MAPPING = {
    "neo4j": "neo4j-python-driver",
}


def get_pypi_main_requirements(filename):
    text = Path(filename).read_text()
    result = PYPI_MAIN_REQUIREMENTS.findall(text)
    return result


def get_pypi_optional_requirements(filename):
    text = Path(filename).read_text()
    text = PYPI_OPTIONAL_GROUP.search(text).group(1)
    result = PYPI_OPTIONAL_REQUIREMENTS.findall(text)
    return result


def get_conda_meta_requirements(filename):
    text = Path(filename).read_text()
    text = CONDA_META_GROUP.search(text).group(1)
    result = CONDA_META_REQUIREMENTS.findall(text)
    return result


def get_conda_post_link_requirements(filename):
    text = Path(filename).read_text()
    result = CONDA_POST_LINK_REQUIREMENTS.findall(text)
    return result


if __name__ == "__main__":
    pypi_requirements = dict(
        get_pypi_main_requirements("requirements.txt")
        + get_pypi_optional_requirements("pyproject.toml")
    )
    conda_requirements = dict(
        get_conda_meta_requirements("conda/meta.yaml")
        + get_conda_post_link_requirements("conda/post-link.sh")
    )

    # Conda specifies Python version alongside other requirements
    del conda_requirements["python"]

    # Same packages may have different names in PyPI and Conda
    for pypi_name, conda_name in PYPI_TO_CONDA_NAME_MAPPING.items():
        pypi_requirements[conda_name] = pypi_requirements[pypi_name]
        del pypi_requirements[pypi_name]

    if pypi_requirements != conda_requirements:
        missing_requirements = []
        differing_requirements = []
        for lib, pypi_version in pypi_requirements.items():
            if lib in conda_requirements:
                conda_version = conda_requirements[lib]
                if conda_version != pypi_version:
                    differing_requirements.append((lib, pypi_version, conda_version))
            else:
                missing_requirements.append(lib)

        print("PyPI and Conda requirements do not match.\n")

        if differing_requirements:
            print("Differing requirements:")
            for lib, pypi_version, conda_version in differing_requirements:
                print(f"- {lib}")
                print(f"    PyPI version: {pypi_version}")
                print(f"    Conda version: {conda_version}")
            print()

        if missing_requirements:
            formatted_missing_requirements = ", ".join(missing_requirements)
            print(f"Missing in Conda requirements: {formatted_missing_requirements}.")

        sys.exit(1)
