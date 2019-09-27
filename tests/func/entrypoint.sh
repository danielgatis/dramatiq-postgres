#!/bin/bash -eux
#
# Entrypoint for both GitLab CI and docker-compose.yml
#

cd $(readlink -m $0/../../..)
test -f pyproject.toml

export PATH=~/.local/bin:$PATH
export XDG_CACHE_HOME=${PWD}/.cache/

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
pip --disable-pip-version-check install --user poetry
poetry install
poetry run flake8 dramatiq_pg/ tests/
poetry run make -C docs/ check
poetry run pytest -x tests/unit/
poetry run tests/pypsql < dramatiq_pg/schema.sql
poetry run tests/pypsql < tests/func/schema.sql
poetry run pytest -x tests/func/
