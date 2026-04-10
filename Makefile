.PHONY: docker
docker:
	@ docker compose up -d

.PHONY: clean
clean:
	@ docker compose down --volumes

.PHONY: dep
dep:
	@ pip3 install -r requirements.txt

.PHONY: run
run: dep
	@ python3 main.py

.PHONY: all
all: docker run
