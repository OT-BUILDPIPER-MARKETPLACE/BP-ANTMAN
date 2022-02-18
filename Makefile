VERSION ?= 0.5
CONF_PATH ?=${PWD}/config/arg.yml

build:
	docker build -t iceman . --network host
run:
	docker run -it --rm --name schedule_rds_redis --network host -v ${CONF_PATH}:/opt/config/arg.yml:ro -e SCHEDULE_ACTION=${ACTION} -e CONF_PATH='/opt/config/arg.yml' -v ~/.aws:/root/.aws iceman 
