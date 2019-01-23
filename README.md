Documentation forthcoming. In the meantime:

## Running with Docker

This can be run in listening mode using Docker, with `openagua/waterlp-pywr`. Several environment variables are needed:

* `AWS_S3_BUCKET`: The root AWS S3 bucket where input/output files are stored.
* `AWS_ACCESS_KEY_ID`: The AWS access key ID for S3 access
* `AWS_SECRET_ACCESS_KEY`: The AWS secret access key for S3 access
* `RABBITMQ_HOST`: The host for listening for new tasks
* `MODEL_KEY`: The unique model key associated with the model (this is used for both logging in to the RabbitMQ host, as well as the queue to listen to).

There are [several ways to pass environment variables to Docker](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file).

If, for example, the variables are stored in a file called `env.list`, the Docker image can be run (in detached mode) as:
```bash
sudo docker run -d --env-file ./env.list openagua/waterlp-pywr
```
