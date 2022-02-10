dockerBuild:
	docker build -f ./Dockerfile -t rudderlabs/rudder-dpcache-layer:v1 .

dockerTag: dockerBuild
	docker tag rudderlabs/rudder-dpcache-layer:v1 us-east1-docker.pkg.dev/rudder-sai/rudder-dpcache-layer/dpcache-layer

dockerPush: dockerTag
	docker push us-east1-docker.pkg.dev/rudder-sai/rudder-dpcache-layer/dpcache-layer:latest
