FLINK_VERSION = 1.17.1

build-scala-image:
	docker build --build-arg FLINK_VERSION=${FLINK_VERSION} -t flink:${FLINK_VERSION}-stream2-no-scala -f Dockerfile .

upload-container:
	docker save flink:${FLINK_VERSION}-stream2-no-scala > uploadContainer.tar
	microk8s images import uploadContainer.tar
	rm uploadContainer.tar

deploy:
	microk8s kubectl create -f deployment.yaml

destroy:
	microk8s kubectl delete deployment value-state

launch:
	flink run-application -p 3 -t kubernetes-application \
		-c modernflink.section2.valueStateDemo \
		-Dtaskmanager.numberOfTaskSlots=2 \
		-Dkubernetes.rest-service.exposed.type=NodePort \
		-Dkubernetes.cluster-id=value-state \
		-Dkubernetes.container.image=flink:${FLINK_VERSION}-stream2-no-scala \
		-Dkubernetes.service-account=flink-service-account \
		local:///opt/flink/usrlib/my-flink-job.jar
