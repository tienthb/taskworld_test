# Taskworld Senior Data Engineer Take Home Test

## For building and running the application you need:
- [Python 3.8.10](https://www.python.org/downloads/release/python-3810/)
- [docker](https://www.docker.com/products/docker-desktop/)

## How to use
Required: docker-compose is installed in your machine. If not, refer to this page for [Docker Compose Installation] (https://docs.docker.com/compose/install/)

1. Create a `.env` file at the root with the following variables:
```sh
POSTGRES_DB=warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_HOST=postgres
```

2. Run this command `docker-compose up`