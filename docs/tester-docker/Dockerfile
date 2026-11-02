# The tester guide's alternative console setup: Deephaven's server image with the library
# installed from the source checkout. Build it from the checkout, as the guide shows:
#   git archive HEAD | docker build -f docs/tester-docker/Dockerfile \
#     --build-arg TESTER_COMMIT=$(git rev-parse HEAD) -t deephaven-massive-test -
# The label records the commit, which `scripts/tester.py check --docker-image` compares with
# the checkout's.
FROM ghcr.io/deephaven/server:42.7
ARG TESTER_COMMIT=unknown
LABEL deephaven-massive.commit=$TESTER_COMMIT
# The library's version comes from git. The image's build context is a `git archive` export,
# which has no .git, so the version comes from .git_archival.txt, which the export fills in.
COPY pyproject.toml README.md LICENSE .git_archival.txt /tmp/deephaven-massive/
COPY src /tmp/deephaven-massive/src
RUN pip install --no-cache-dir /tmp/deephaven-massive && rm -rf /tmp/deephaven-massive
