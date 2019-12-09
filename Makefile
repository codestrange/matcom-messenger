.DEFAULT_GOAL := help

help: ## Shows Makefile help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

flask_run: ## Run flask client app
	export FLASK_APP=client/run.py &&\
    export FLASK_ENV=production    &&\
    export FLASK_DEBUG=0           &&\
    flask run

tracker_run: ## Run Tracker app
	python tracker.py

run: ## Run all app services
	python app.py
