# OpAMP Benchmark Kubernetes deployment

## Prerequisites

- Access to a Docker registry to publish the OpAMP docker image.
- Access to a kubernetes cluster, or two clusters in case if you want to
  benchmark OpAMP in a distributed environment.

## Quickstart

1. Build docker image that includes executables for both client and server.
   
   ```bash
   make docker-build
   ```

2. Push the docker image to your docker registry.
   
   ```bash
   docker tag opamp <YOUR_DOCKER_REGISTRY>/opamp:latest
   docker push <YOUR_DOCKER_REGISTRY>/opamp:latest
   ```

3. Deploy OpAMP server. 

   ```bash
   cp examples/kubernetes/opamp-server/values.yaml examples/kubernetes/opamp-server/custom-values.yaml
   ```

   Open `examples/kubernetes/opamp-server/custom-values.yaml` and adjust
   parameters. Make sure `image.repository` is provided. K8s ClusterIP `service`
   type can be changed to `LoadBalancer` in order to run the benchmarks between
   different clusters.

   Then deploy the OpAMP server with the following command:
   
   ```bash
   helm install my-opamp-server ./examples/kubernetes/opamp-server/ -f ./examples/kubernetes/opamp-server/custom-values.yaml
   ```

   Any time server parameters needs to be adjusted, just change
   `examples/kubernetes/opamp-server/custom-values.yaml` and redeploy OpAMP
   server:

   ```bash
   helm upgrade my-opamp-server ./examples/kubernetes/opamp-server/ -f ./examples/kubernetes/opamp-server/custom-values.yaml
   ```
   
   If you want to access the pprof endpoint exposed by the server you need to enable
   port forwarding, e.g.:
   ```bash
   kubectl port-forward my-opamp-server-658f64f6d7-j7vsn 6060
   ```
   Now you can access http://localhost:6060/debug/pprof/

5. Deploy OpAMP load generator client. 

   ```bash
   cp examples/kubernetes/opamp-client/values.yaml examples/kubernetes/opamp-client/custom-values.yaml
   ```

   Open `examples/kubernetes/opamp-client/custom-values.yaml` and adjust
   parameters. Make sure `image.repository` is provided. Also update `endpoint`
   if OpAMP Server is deployed in another cluster behind a load balancer.

   Then deploy the OpAMP client with the following command:
   
   ```bash
   helm install my-opamp-client ./examples/kubernetes/opamp-client/ -f ./examples/kubernetes/opamp-client/custom-values.yaml
   ```

   Any time client parameters needs to be adjusted, just change
   `examples/kubernetes/opamp-client/custom-values.yaml` and redeploy OpAMP
   client:

   ```bash
   helm upgrade my-opamp-client ./examples/kubernetes/opamp-client/ -f ./examples/kubernetes/opamp-client/custom-values.yaml
   ```
