# Firebolt Large Schema Benchmarking

This repository contains some benchmarking scripts for dealing with large schemas on Firebolt.
The script sets up 1000 databases (configurable) with 100 tables each, and then benchmarks query performance.

We test the latencies of queries while continuously modifying the schema.
We differentiate between two types of schema modification here:
- Modification of the database we run queries on
- Modification of a different database from the one we run queries on

For each scenario, we generate a box-plot for the 1st, 2nd and 3rd query run after the schema modification (configurable).
We can see that while the first query can have an overhead of up to ~60ms, for all subsequent queries the latency stabilizes.

## Setup 
You need to modify the `main.py` script in order to run this on Firebolt.
On your Firebolt account, create a service account and make sure it has RBAC permissions to create/drop
engines, databases and tables.

Take the service account credentials, as well as your account name and add them to the `main.py` script.

## Running
Make sure you have `python3` and `pip3` installed, then run:
```sh
pip3 install -r requirements.txt
python3 main.py
```

## Results
The script generates a set of result `pdf` plots that contain boxplots for the client-measured latency.
Note that if your client runs far away from the Firebolt engine (e.g. in Europe while the account is in the US),
you will see the network round-trip latency make up most of the client-measured time.

## Sample Results
We include sample results in the `sample_results` folder of this repository.
They were measured on Firebolt version 4.9.7 (run `SELECT version()` on your engine to get the Firebolt version).
Both the Firebolt engine as well as the measurement script were deployed in `us-east-1`.

