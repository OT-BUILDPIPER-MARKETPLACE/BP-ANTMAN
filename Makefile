VERSION ?= 1.0
CONF_PATH ?=${PWD}/config/resize_aws_resources_sample.yml

build:
	docker build -t opstree/antman:${VERSION} .
run:
	docker run -it --rm --name antman -v ${CONF_PATH}:/opt/config/resize_aws_resources_sample.yml:ro -e SCHEDULE_ACTION=${ACTION} -e CONF_PATH='/opt/config/resize_aws_resources_sample.yml' -v ~/.aws:/root/.aws opstree/antman:${VERSION} 
