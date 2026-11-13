# 44. Releases are one manual workflow run; PyPI through trusted publishing, off until the owner turns it on

Date: 2026-11-02

Status: accepted

The run tests `main`, tags `v<version>`, and creates the GitHub Release; the git tag is the only
version source, and there is no hand-written changelog.
