### Prerequisites

Python 3.6.* or later.

Check if python3 is installed:

```
python3 --version
```

### Dependencies and data

#### Creating a virtual environment

Ensure the pip package manager is up to date:
```
pip3 install --upgrade pip
```

From the root directory of the project, create the virtual environment:

```
python3 -m venv ./venv
```

Activate the newly created virtual environment

```
source ./venv/bin/activate
```


#### Installing Python requirements

This will install the packages required for the application to run:

```
pip3 install -r ./solution/requirements.txt
```


#### Setting up the input data

This will unzip the input data folder:
```
unzip ./input-example.zip -d ./solution/
```

### Running the application and the tests

#### Running the application
```
python3 ./solution/app/main.py
```

#### Running tests to ensure everything is working correctly
```
pytest -vv ./solution/tests
```


#### Clean-up
```
rm -r ./venv
rm -r ./solution/input
rm -r ./solution/output
```
