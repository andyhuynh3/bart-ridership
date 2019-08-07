run:
	python bart/app.py

lint:
	flake8 --exclude=.tox

format:
	black .

dcb:
	docker-compose build

dcu:
	docker-compose up -d

clean:
	docker-compose down
	docker system prune -fa
