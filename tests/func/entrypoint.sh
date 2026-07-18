#!/bin/bash -eux
#
# Entrypoint for docker-compose.yml
#

cd $(readlink -m $0/../../..)
test -f pyproject.toml

export PATH=~/.local/bin:$PATH
export XDG_CACHE_HOME=${PWD}/.cache/
export SKIP_TESTCONTAINERS=${SKIP_TESTCONTAINERS-1}

teardown() {
    # If not on CI, wait for user interrupt on exit
    if [ -z "${CI-}" -a $? -gt 0 -a $PPID = 1 ] ; then
        : Container failed. Debug with:
        : "    docker exec -it $HOSTNAME /bin/bash"
        tail -f /dev/null
    fi
    sudo chown -R $owner $XDG_CACHE_HOME
}

owner=$(stat -c %u .)
runner=$(id -u)
trap teardown EXIT TERM

if [ $runner -gt 0 -a $owner -ne $runner ] ; then
    exec sudo -E $0
fi
sudo mkdir -p $XDG_CACHE_HOME
sudo chown -R $runner $XDG_CACHE_HOME

mkdir -p $XDG_CACHE_HOME
uv sync
# For now, just run unit test along func tests.
uv run pytest -x tests/unit/
uv run dramatiq-postgres init
uv run tests/pypsql < tests/func/schema.sql
uv run pytest -x tests/func/
