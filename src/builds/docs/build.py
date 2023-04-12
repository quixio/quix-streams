import os

os.system("docker build . -f ./dockerfile --tag quix-docs-base-img")

os.system(f"docker run -it -v \"$(pwd)/../../..:/QuixStreams\" quix-docs-base-img")