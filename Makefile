SHELL := /bin/bash

test:
	./deploy_tests.sh

build:
	./deploy_build_concourse.sh

publish: test build
	./deploy_publish.sh
