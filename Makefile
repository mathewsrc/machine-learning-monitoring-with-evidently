setup:
	@echo "Setting up virtual environment"
	python -m venv ~/.env

install:
	@echo "Installing dependencies"
	pip install --upgrade pip  &&\
		pip install -r requirements.txt

format:
	@echo "Formatting all projects with black"
	./format.sh

lint:
	@echo "Linting all projects with ruff"
	./lint.sh

test:
	@echo "Testing all projects with pytest"
	./test.sh

mlflow-init:
	@echo "Starting MLflow"
	mlflow ui

mlflow-custom:
	@echo "Starting MLflow with custom parameters"
	bash ./start-mlflow-server.sh $(PORT) $(HOST) $(ARTIFACTS)

mlflow-clear:
	@echo "Clearing MLflow files"
	rm -rf ./mlflow.db ./artifacts ./mlruns

mlflow-delete-experiments:
	@echo "Deleting all experiments"
	mlflow gc

mlflow-serve:
	@echo "Starting MLflow with custom parameters"
	bash ./start-mlflow-server.sh $(PORT) $(HOST) $(ARTIFACTS)
