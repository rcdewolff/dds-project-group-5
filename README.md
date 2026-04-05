# Distributed Data Systems group 5

The main branch of this repository contains both a 2PC and SAGA implementation.

Command to run SAGA/2pc version (different machine strengths):

```bash
docker compose -f saga/docker-compose.small.yml up -d --build --wait

docker compose -f saga/docker-compose.50cpu.yml up -d --build --wait

docker compose -f saga/docker-compose.90cpu.yml up -d --build --wait

docker compose -f 2pc/docker-compose.small.yml up -d --build --wait

docker compose -f 2pc/docker-compose.50cpu.yml up -d --build --wait

docker compose -f 2pc/docker-compose.90cpu.yml up -d --build --wait
```

Command to shut down SAGA/2pc version (replace saga with 2pc for 2pc):

```bash
docker compose -f saga/docker-compose.yml down

# Also remove volumes
docker compose -f saga/docker-compose.yml down -v

# Also remove volumes and service images
docker compose -f saga/docker-compose.yml down -v --rmi local

# Also remove volumes and all images
docker compose -f saga/docker-compose.yml down -v --rmi all
```

## Note on 2PC commit history

The commit history for the 2 phase commit implementation on the main branch may be inaccurate before March 16th 2026, as merging the two implementations using git proved difficult. For an accurate commit history up to March 16th 2026, checkout the branch '2pc-adithya'.

## Original template README: Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
