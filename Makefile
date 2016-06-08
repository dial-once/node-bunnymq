.PHONY: test
run:
	node src/index.js
deps:
	npm install -g jshint mocha istanbul
	npm i
lint:
	jshint .
test:
	make lint
	make cover
init:
	sed -i 's/{service-name}/$(NAME)/g' package.json
	sed -i 's/{service-name}/$(NAME)/g' sonar-project.properties
	sed -i 's/{service-name}/$(NAME)/g' README.md
cover:
	make lint && \
	docker stop rabbitmq-bunnymq && docker rm rabbitmq-bunnymq; true && \
	docker run -d --name=rabbitmq-bunnymq -p 5672:5672 rabbitmq:3.6 && sleep 2 && \
	mocha test --recursive && \
	docker stop rabbitmq-bunnymq
sonar:
	sed '/sonar.projectVersion/d' ./sonar-project.properties > tmp && mv tmp sonar-project.properties
	echo sonar.projectVersion=`cat package.json | python -c "import json,sys;obj=json.load(sys.stdin);print obj['version'];"` >> sonar-project.properties
	wget http://repo1.maven.org/maven2/org/codehaus/sonar/runner/sonar-runner-dist/2.4/sonar-runner-dist-2.4.zip
	unzip sonar-runner-dist-2.4.zip
ifdef CI_PULL_REQUEST
	sonar-runner-2.4/bin/sonar-runner -e -Dsonar.analysis.mode=preview -Dsonar.github.pullRequest=${shell basename $(CI_PULL_REQUEST)} -Dsonar.github.repository=$(REPO_SLUG) -Dsonar.github.oauth=$(GITHUB_TOKEN) -Dsonar.login=$(SONAR_LOGIN) -Dsonar.password=$(SONAR_PASS) -Dsonar.host.url=$(SONAR_HOST_URL)
endif
ifeq ($(CIRCLE_BRANCH),develop)
	@sonar-runner-2.4/bin/sonar-runner -e -Dsonar.analysis.mode=publish -Dsonar.host.url=$(SONAR_HOST_URL) -Dsonar.login=$(SONAR_LOGIN) -Dsonar.password=$(SONAR_PASS)
endif
	rm -rf sonar-runner-2.4 sonar-runner-dist-2.4.zip

