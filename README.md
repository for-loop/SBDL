## Install

### Python 3.10

Check python version. If not 3.10, install it

```bash
brew install python@3.10
```

### Virtual env

Here's how to use `venv` (instead of `pipenv`)

```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

### Dependencies

In `requirements.txt`, add the following (the content is taken from `Pipfile` for `pipenv`):

```
pyspark==3.3.0
pytest
black
```

Then execute

```bash
pip install -r requirements.txt
```

After installed, get the list of requirements by

```bash
pip freeze
```

and replace contents of `requirements.txt`

### Run

Select `sbdl_main.py` and click Run button. The following error will show:

```bash
Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing
```

Append ` local 2022-08-02` and execute the command again

### Unit test

```bash
pytest
```