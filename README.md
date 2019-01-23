Documentation forthcoming. In the meantime:

## Running with Docker

This can be run in listening mode using Docker, with `openagua/waterlp-pywr`. Several environment variables are needed:

* `AWS_S3_BUCKET`: The root AWS S3 bucket where input/output files are stored
* `AWS_ACCESS_KEY_ID`: The AWS access key ID for S3 access
* `AWS_SECRET_ACCESS_KEY`: The AWS secret access key for S3 access
* `RABBITMQ_HOST`: The host for listening for new tasks
* `MODEL_KEY`: The unique model key associated with the model (this is used for both logging in to the RabbitMQ host, as well as the queue to listen to)
* `RUN_KEY`: A unique key associated with the model run in OpenAgua; for the time being, this should be the model run name.

There are [several ways to pass environment variables to Docker](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file).

If, for example, the variables are stored in a file called `env.list`, the Docker image can be run (in detached mode) as:
```bash
sudo docker run -d --env-file ./env.list --name waterlp openagua/waterlp-pywr
```

To stop and remove this container (for example to update the docker image):
```bash
sudo docker rm --force waterlp
```

These can be combined into a single bash script to upgrade the container (killing the old container):

```bash
sudo docker rm --force waterlp
sudo docker run -d --env-file ./env.list --name waterlp openagua/waterlp-pywr
```