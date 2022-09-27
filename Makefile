SHELL := /bin/bash

test:
       ./deploy_tests.sh

build:
       ./deploy_build.sh

publish: test build
       ./deploy_publish.sh
