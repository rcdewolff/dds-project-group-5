# Distributed Data Systems Project Template

Saga-critical communication is implemented with Kafka and database-backed Inbox/Outbox reliability.
Redis is optional and not used in the saga-critical checkout progression path.

## Persistence model

The Saga implementation now uses conventional current-state relational tables as the source of truth:

- Payment service stores user balances in `accounts`.
- Stock service stores item stock/price in `inventory`.
- Order service stores business state in `orders` and saga progression state in `sagas`.

Inbox and Outbox tables are preserved in all services for idempotent consumption and reliable Kafka publication.

Legacy `log`/`*_snapshots` tables are not used as primary domain state anymore and are no longer created by
service startup logic.

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

The compose setup includes:
- `kafka-init`: manually creates application topics (`stock.request`, `payment.request`, `order.request`, `checkout-commands`, `checkout-results`) with 6 partitions and replication factor 1
- dedicated `*-consumer` and `*-producer` containers for Kafka consumption and outbox publication
- `order-checkout-worker` (Uvicorn/FastAPI) as the async HTTP worker for `/orders/checkout/*`

Checkout worker flow (Kafka waiter):
- `POST /orders/checkout/{order_id}` generates `correlation_id`, registers local waiter, and emits `checkout.command` to Kafka topic `checkout-commands`
- order consumer starts the saga using the command `correlation_id`
- order checkout workflow supplies the concrete saga steps and routing targets to `order/orchestrator.py`; the orchestrator only persists state, dispatches the provided step messages, and reports terminal completion/failure back to `checkout-results`
- checkout-worker waits in-memory for a matching terminal event from Kafka topic `checkout-results`
- terminal `completed` returns HTTP 200; terminal `failed`/`compensated` returns HTTP 400
- request remains open until terminal result arrives (no polling, no 202/504 timeout response)

Checkout worker env vars:
- `CHECKOUT_MAX_INFLIGHT`
- `KAFKA_BOOTSTRAP_SERVERS`
- `CHECKOUT_COMMANDS_TOPIC`
- `CHECKOUT_RESULTS_TOPIC`
- `CHECKOUT_WORKER_GROUP_ID` (base id; worker appends host/pid for per-process uniqueness)

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
