Install poetry

## Run in development

Run `poetry install --no-root` to install dependencies without specifying root.

Run `poetry run python src/main.py` if you want to start analyzing manually added wallets from wallets.csv.

Run `poetry run python src/app.py` to start flask app. It is available on `http://127.0.0.1:8000`.

## Run in docker container

Run `make update-deps` to create requirements.txt with poetry deps required for docker image.

Run `docker-compose build` to build the Docker image.

Run `docker-compose up` to run the Docker container.

It is avaiable on `http://localhost:8080`.
