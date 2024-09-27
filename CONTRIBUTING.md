# Contributing to Quix Streams

Thanks for considering contributing! We would love your input. We want to make contributing to this project as easy and transparent as possible, whether that's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

If you want to participate more in-depth, for example, extending the code base, reach out to us on [The Stream](https://quix.io/slack-invite) community. We'll show you the current architecture and you can suggest changes.

## Create an issue

Before making significant contributions to this project, consider outlining your solution first. You can do this by [generating an issue](/issues) in the form of a bug report or a feature request.

Issues should be used to report problems with the library, request a new feature, or to discuss potential changes before a PR is created. When you create a new Issue, a template will be loaded that will guide you through collecting and providing the information we need to investigate.

If you find an issue that describes the problem you're having, please add your own instructions on how to reproduce it to the existing issue, rather than creating a new one. Adding a [reaction](https://github.blog/2016-03-10-add-reactions-to-pull-requests-issues-and-comments/) can also help indicate to our maintainers that a particular problem is affecting more than just the reporter.

## Pull Requests

PRs to our library are always welcome and can be a quick way to get your fix or improvement scheduled for the next release. In general, PRs should:

- Fix/add a functionality that has been reported first through an [issue](/issues).
- Address a single concern in the least number of changed lines as possible.
- Add unit or integration tests for fixed or changed functionality if needed.
- Update the [documentation](/docs) of the library if needed.

For changes that address core functionality or would require breaking changes (e.g. a major release), it's best to open an [Issue](/issues) to discuss your proposal first before starting coding your solution.

In general, we follow the ["fork-and-pull" Git workflow](https://github.com/susam/gitpr)

1. Fork the repository to your own GitHub account.
2. Clone the project to your machine.
3. Create a branch locally with a succinct but descriptive name.
4. Commit changes to the branch.
5. Following any formatting and testing guidelines specific to this repo.
6. Push changes to your fork.
7. Open a PR in our repository and follow the PR template so that we can efficiently review the changes.

## Coding standards

To keep the code as consistent as possible, please familiarize yourself with the existing style of the project. If you're contributing to the Python code base, follow the [PEP 8 - Style Guide for Python Code](https://peps.python.org/pep-0008/). 

>  Consistency within a project is important. Consistency within one module or function is the most important.

## Setting up the project locally

This guide will assume you're using Visual Studio Code, but most of the guide is applicable if you're using other IDEs also.

1. Navigate to root of the project
2. Execute `python3 -m pip install -r requirements.txt -r requirements-dev.txt -r tests/requirements.txt`
3. Ensure you don't have `quixstreams` installed using `python3 -m pip uninstall quixstreams` to avoid using that when testing
4. By executing `python3 -m pip install --editable .` you will be able to use the source code as a module for local testing without having to install new versions
5. Add commit pre-hook: `pre-commit install`
6. Run test from project root with `pytest`
