Documentation forthcoming. In the meantime:

## Requirements

WaterLP requires Redis, which may be installed independently from WaterLP with:

```bash
docker run --name oa-redis -d redis
```

## Running with Docker

WaterLP can be run in listening mode using Docker, with `openagua/waterlp-pywr`.

The general process for running with Docker is:
1. Download (*pull*) the Docker image to the local machine
2. Run a Docker container from the image.

To update the image/container, any existing running container should be removed. This is demonstrated in the example below.

### Download the image

Download the image with:
```bash
docker pull openagua/waterlp-pywr
```

### Run the container

Running the container from the images is done with one command, with several options. The general format for running a container is:
```bash
docker run [options] openagua/waterlp-pywr
```

The WaterLP Docker container should be run with at least the following options:
1. detached mode (optional): `-d`
2. environment variables
3. a log file directory
4. a mapping of the local time zone information to the container

These are described, with a specific example Docker run command and fuller sequence of commands to make sure the Docker contiainer is up-to-date.

**Environment variables**

Several environment variables are needed:

* `AWS_ACCESS_KEY_ID`: The AWS access key ID for S3 access
* `AWS_SECRET_ACCESS_KEY`: The AWS secret access key for S3 access
* `RABBITMQ_HOST`: The host for listening for new tasks
* `MODEL_KEY`: The unique model key associated with the model (this is used for both logging in to the RabbitMQ host, as well as the queue to listen to)
* `RUN_KEY`: A unique key associated with the model run in OpenAgua; for the time being, this should be the model run name.

There are [several ways to pass environment variables to Docker](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file).

**Log file**

WaterLP creates log files that can be saved on the host machine by mapping the WaterLP log file directory to the host machine directory via the `--volume` flag.

In general, the logfile will be stored in the user's home directory, under `~/.waterlp/logs`. To map this location to the container: `--volume /home/\[user\]:/home/root`, since the container is run as root.

**Time zone information**

Dates--in particular Pendulum--need to know the machine time zone information. This is achieved with `--volume /etc/localtime:/etc/localtime`, assuming the host machine is Ubuntu. If not, further investigation is needed to pass this info.

### Example: Linux (Ubuntu)

In this example, variables will be stored in a file called `env.list`, and log files will be stored in `/log/waterlp` on the host machine. The container will be named `waterlp`, so we can delete it easily, and will be run in "detached" mode using the `-d` flag. 

With `ubuntu` as the example user (as on an Amazon EC2 Ubuntu instance), the container is run with:
```bash
docker run -d --env-file ./env.list --link oa-redis:redis --volume /home/ubuntu:/home/root --volume /etc/localtime:/etc/localtime  --name waterlp openagua/waterlp-pywr
```

To stop and remove this container (for example to update the docker image):
```bash
sudo docker rm --force waterlp
```

These can be brought together into a single set of commands to start/update the container, such as in a bash script.

Since updating images in this way results in an accumulation of unused images, it can be useful to remove old images. This is achieved with the following command:
```bash
docker image prune --all --force
```
where `--all` removes all unused images and `--force` does not prompt for user confirmation (this is optional if running this command manually). See [docker image prune](https://docs.docker.com/engine/reference/commandline/image_prune/).

These can be brought together in a bash script, as follows:

```bash
docker run --name oa-redis -d redis
docker pull openagua/waterlp-pywr
docker rm --force waterlp
docker run -d --env-file ./env.list --link oa-redis:redis --volume /home/ubuntu:/home/root --volume /etc/localtime:/etc/localtime  --name waterlp openagua/waterlp-pywr
docker image prune --all --force
```

This is included in `run_docker_image.sh` in the `scripts` folder (without `sudo`).

### Example: Windows

A similar example can be developed for Windows. First, please make sure to [turn on the appropriate shared drive](https://docs.docker.com/docker-for-windows/#shared-drives).

```
docker pull openagua/waterlp-pywr:latest
docker rm --force waterlp
docker run -d --env-file //Users/david/Documents/waterlp/env.txt --volume /home/ubuntu://Users/david/Documents/waterlp --name waterlp openagua/waterlp-pywr
docker image prune --all --force
```
