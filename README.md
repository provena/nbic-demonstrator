# nbic-demonstrator

Toy example from stripped down NBIC workflow demonstrating the usage of Provena for registering notebook based provenance.

## Instructions

Install system dependencies using apt-get (graphviz and libs) (linux based system)

```
./dependencies.sh
```

Install python venv

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the `hourly_FFDI.ipynb` notebook.

## Workflow configuration

The `workflow_config.py` file contains the Pydantic model which the specified workflow file must satisfy.

You can run this file (using the main method) to generate a template file in the `configs` directory.

There is an example workflow file provided which is currently used in the notebook.
