Documentation forthcoming. In the meantime:

## Running with Docker

Several options can be passed when starting a Docker container. The WaterLP Docker container should be run with at least:
1. environment variables
2. a log file directory

This can be run in listening mode using Docker, with `openagua/waterlp-pywr`. Several environment variables are needed:

* `AWS_S3_BUCKET`: The root AWS S3 bucket where input/output files are stored
* `AWS_ACCESS_KEY_ID`: The AWS access key ID for S3 access
* `AWS_SECRET_ACCESS_KEY`: The AWS secret access key for S3 access
* `RABBITMQ_HOST`: The host for listening for new tasks
* `MODEL_KEY`: The unique model key associated with the model (this is used for both logging in to the RabbitMQ host, as well as the queue to listen to)
* `RUN_KEY`: A unique key associated with the model run in OpenAgua; for the time being, this should be the model run name.

There are [several ways to pass environment variables to Docker](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file).

WaterLP creates log files that can be saved on the host machine by mapping the WaterLP log file directory to the host machine directory via the `--volume` flag.

### Example:

In this example, variables will be stored in a file called `env.list`, and log files will be stored in `/log/waterlp` on the host machine. The container will be named `waterlp`, and will be run in "dettached" mode using the `-d` flag. 

The container with these options is run with:
```bash
sudo docker run -d --env-file ./env.list --volume /log:/log  --name waterlp openagua/waterlp-pywr
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