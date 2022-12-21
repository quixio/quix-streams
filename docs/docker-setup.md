# Set up docker environment

Docker is an alternative to the classic environment setup for local
development ( see [Set up your local IDE](/sdk/python-setup) )

!!! note

	Docker knowledge is not required to develop or deploy applications with Quix. This guide is only for people that prefer using local development environments using Docker.

It enables you to install and run your project in isolation from the
rest of the system which provides the advantage of removing system
dependencies and conflicts.

Isolation is achieved by creating a lightweight container (Docker
Container) which behaves like a virtual machine (in our case Ubuntu
Linux).

## Install Prerequisities

In order to use the Quix SDK in Docker you need to have installed these
prerequisities.

  - Docker (tested on version 20.10.17)

  - Docker Compose (tested on version 1.29.2)

### Install Docker ( step 1 )

To install the Docker on your environment you need to follow this guide
[here
(<https://docs.docker.com/get-docker/>)](https://docs.docker.com/get-docker/).

!!! note

	On Windows we tested this setup using the WSL2 backend.

### Install docker-compose ( step 2 )

We’ll be using the docker-compose tool which is designed for easy
configuration of local Docker setups.

To install docker-compose, please follow the guide [here (
<https://docs.docker.com/compose/install/>
)](https://docs.docker.com/compose/install/).

!!! note

	If you are on Windows then you can skip this step because the Docker installation package from the step 1 already contains the docker-compose tool.

## Start with downloading a couple of things

  - Download the project then navigate to your solution’s root folder.

  - Navigate to
    [quix-library](https://github.com/quixai/quix-library/tree/main/python/local-development){target=_blank}
    and download `/docker/` folder content.

## Build and run project

Open a command line within your new `docker` folder.

You can start the build by running the following command.

``` bash
docker-compose run --rm server
```

!!! note

	On the first run the compile script may take while (around 10 minutes) to build all the project dependencies. The subsequent builds will be much faster.

Using the docker file provided, you should now be in a running server,
which has all requirements installed for Quix. As the running image is
nothing more than the `quixpythonbaseimage` with your code folder
mounted at `/app`, in order to get your application working, you’ll need
to install your python requirements.

You can do this using the following, executed in the `/app` folder

``` bash
python3 -m pip install -r requirements.txt --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
```

Use the resulting environment as you would your own machine, such as run
your python application by executing `python3 main.py`

!!! note

	As `/apps` folder is a mounted directory, any file or folder change in the container will be synced to your original folder in your machine and vice-versa.

!!! note

	As your environment variables will greatly depend on what your application needs, make sure to update `docker/.env` as needed. By default all values are placeholder and this might be something you need to configure or add to before the application can correctly run. Several of these environment values could be considered "secrets", therefore be mindful of what you end up committing to your repository.

### Additional documentation

To get the additional information on Docker and Docker compose commands
please follow up with the documentation:

  - [Docker documentation ( <https://docs.docker.com/reference/>
    )](https://docs.docker.com/reference/)

  - [Docker compose documentation ( <https://docs.docker.com/compose/>
    )](https://docs.docker.com/compose/)
