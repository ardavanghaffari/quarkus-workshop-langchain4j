#!/bin/bash -e

cd "$(dirname "$0")/docs"
pipenv run mkdocs build --clean
pipenv run mkdocs serve --livereload
