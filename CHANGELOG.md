# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project uses [Semantic Versioning](http://semver.org/).


# [3.0.1] - 2020-07-24
# Fixed
 - fixed readthedocs link + build
 - re-compiled dependencies to fix security issue in a pinned dependency
 - removed requirements-read-the-docs.txt (no longer needed now that stork is installed in requirements-dev.txt)

# [3.0.0] - 2020-07-23
# Changed
 - renamed repo to stork

========= prior development under the name apparate =========

# [2.3.0] - 2020-07-22
# Added
 - repo rename warning
 - github actions for CI/CD steps

# [2.2.3] - 2020-06-15
# Changed
 - add repo name note
 - define requirements with pip-tools

# [2.2.2] - 2019-02-14
# Fixed
 - allow user to specify path to config file when running pytest

# [2.2.1] - 2019-02-14
# Fixed
 - added license file to setup.py so apparate can be installed from tarball
 - added note to docs that apparate only works on AWS

# [2.2.0] - 2018-11-15
### Added
 - Support for loading jars
 - DEBUG logging
### Changed
 - Moved print statements to INFO logging
 - Updated dependency versions

# [2.1.0] - 2018-10-11
### Added
 - Now with deployment pipeline!
 - Fixes markdown rendering on PyPi

# [2.0.1] - 2018-10-10
### Fixed
 - Req file and link changes for hosting documentation on readthedocs.io

# [2.0.0] - 2018-10-10
### Added
 - Initial open-source release of apparate!
