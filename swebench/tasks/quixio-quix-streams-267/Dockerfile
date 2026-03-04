FROM python:3.11

RUN apt-get update && apt-get install -y \
    git curl ca-certificates patch build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

RUN git clone https://github.com/quixio/quix-streams.git /workspace/repo && cd /workspace/repo && \
    BASE=abdeab8365db3d22024a513aebdc2158c16e034a && \
    cur_branch="$(git rev-parse --abbrev-ref HEAD)" && \
    git reset --hard "$BASE" && \
    git for-each-ref --format='%(refname:short)' refs/heads | while read -r b; do \
      [ "$b" = "$cur_branch" ] || git branch -D "$b"; \
    done && \
    git remote | xargs -r -n1 git remote remove && \
    rm -f .git/config.lock && \
    git reflog expire --expire=now --all && \
    git gc --prune=now --aggressive

WORKDIR /workspace/repo

RUN pip install --upgrade pip setuptools wheel

RUN pip install -r requirements.txt

RUN pip install -r tests/requirements.txt

RUN pip install -e .

ENV PYTHONPATH=/workspace/repo:$PYTHONPATH

WORKDIR /workspace
