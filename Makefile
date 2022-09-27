SHELL := /bin/bash

test:
	./deploy_tests_concourse.sh

build:
	./deploy_build_concourse.sh

publish: test build
	./deploy_publish_concourse.sh
