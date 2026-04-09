.PHONY: docker
docker:
	@ docker compose up -d

.PHONY: clean
clean:
	@ docker compose down --volumes

.PHONY: run
run:
	@ python3 main.py

.PHONY: all
all: docker run
