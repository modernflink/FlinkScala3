build-scala-image:
	docker build -t flink:1.17.0-stream2-no-scala -f Dockerfile .

upload-container:
	docker save flink:1.17.0-stream2-no-scala > uploadContainer.tar
	microk8s images import uploadContainer.tar
	rm uploadContainer.tar

deploy:
	microk8s kubectl create -f deployment.yaml

destroy:
	microk8s kubectl delete flinkdeployment flink-word-count

launch:
	flink run-application -p 3 -t kubernetes-application \
		-c com.example.wordCount \
		-Dtaskmanager.numberOfTaskSlots=2 \
		-Dkubernetes.rest-service.exposed.type=NodePort \
		-Dkubernetes.cluster-id=flink-word-count \
		-Dkubernetes.container.image=flink:1.17.0-stream2-no-scala \
		-Dkubernetes.service-account=flink-service-account \
		local:///opt/flink/usrlib/my-flink-job.jar
