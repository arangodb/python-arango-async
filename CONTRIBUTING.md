# Contributing

Set up dev environment:
```shell
cd ~/your/repository/fork  # Activate venv if you have one (recommended)
pip install -e .[dev]      # Install dev dependencies (e.g. black, mypy, pre-commit)
pre-commit install         # Install git pre-commit hooks
```

Run unit tests with coverage:

```shell
pytest --cov=arango --cov-report=html  # Open htmlcov/index.html in your browser
```

To start and ArangoDB instance locally, run:

```shell
./starter.sh  # Requires docker
```

Build and test documentation:

```shell
python -m sphinx docs docs/_build  # Open docs/_build/index.html in your browser
```

Thank you for your contribution!
