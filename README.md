# SBDL

### An ETL using PySpark

This app extracts source data in either Hive tables or CSV files and transforms into JSON.

The three source data looks like this:

* Accounts data

    load_date | active_ind | account_id | source_sys | account_start_date | legal_title_1 | legal_title_2 | tax_id_type | tax_id | branch_code | country
    -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | --
    2022-08-02 | 1 | 6982391060 | COH | 2018-03-24T13:56:45.000+05:30 | Jane Doe | John Doe | EIN | ABCD01234567898765 | ABCDEFG1 | Mexico

* Parties data

    load_date | account_id | party_id | relation_type | relation_start_date
    -- | -- | -- | -- | --
    2022-08-02 | 6982391060 | 9823462810 | F-N | 2019-07-29T06:21:32.000+05:30

* Addresses data

    load_date | party_id | address_line_1 | address_line_2 | city | postal_code | country_of_address | address_start_date
    -- | -- | -- | -- | -- | -- | -- | --
    2022-08-02 | 9823462810 | 45229 Drake Route | 13306 Corey Point | Shanefort | 77163 | Canada | 2019-02-26

The transformed data will combine all three data by `account_id` and `party_id` where `active_ind` is `1`. The format is JSON with headers with unique UUID and current timestamp for each record.

* Transformed data

    ```json
    {
        "eventHeader": {
            "eventIdentifier": "1841fa29-dcf1-457a-9d37-91136f1fd8e1",
            "eventType": "SBDL-Contract",
            "majorSchemaVersion": 1,
            "minorSchemaVersion": 0,
            "eventDateTime": "2024-08-15T13:47:47.390-07:00"
        },
        "keys": [
            {
                "keyField": "contractIdentifier",
                "keyValue": "6982391060"
            }
        ],
        "payload": {
            "contractIdentifier": {
                "operation": "INSERT",
                "newValue": "6982391060"
            },
            "sourceSystemIdentifier": {
                "operation": "INSERT",
                "newValue": "COH"
            },
            "contactStartDateTime": {
                "operation": "INSERT",
                "newValue": "2018-03-24T01:26:45.000-07:00"
            },
            "contractTitle": {
                "operation": "INSERT",
                "newValue": [
                    {
                        "contractTitleLineType": "lgl_ttl_ln_1",
                        "contractTitleLine": "Jane Doe"
                    },
                    {
                        "contractTitleLineType": "lgl_ttl_ln_2",
                        "contractTitleLine": "John Doe"
                    }
                ]
            },
            "taxIdentifier": {
                "operation": "INSERT",
                "newValue": {
                    "taxIdType": "EIN",
                    "taxId": "ABCD01234567898765"
                }
            },
            "contractBranchCode": {
                "operation": "INSERT",
                "newValue": "ABCDEFG1"
            },
            "contractCountry": {
                "operation": "INSERT",
                "newValue": "Mexico"
            },
            "partyRelations": [
                {
                    "partyIdentifier": {
                        "operation": "INSERT",
                        "newValue": "9823462810"
                    },
                    "partyRelationshipType": {
                        "operation": "INSERT",
                        "newValue": "F-N"
                    },
                    "partyRelationStartDateTime": {
                        "operation": "INSERT",
                        "newValue": "2019-07-28T17:51:32.000-07:00"
                    },
                    "partyAddress": {
                        "operation": "INSERT",
                        "newValue": {
                            "addressLine1": "45229 Drake Route",
                            "addressLine2": "13306 Corey Point",
                            "addressCity": "Shanefort",
                            "addressPostalCode": "77163",
                            "addressCountry": "Canada",
                            "addressStartDate": "2019-02-26"
                        }
                    }
                }
            ]
        }
    }
    ```

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

## Run locally

```bash
python sbdl_main.py local -d 2022-08-02
```

### Help

```bash
python sbdl_main.py -h
```

## Unit test

```bash
pytest
```

---

This is a capstone project as a part of Udemy's [PySpark - Apache Spark Programming in Python for beginners](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/). The app is built from this [GitHub starter repo](https://github.com/LearningJournal/Spark-Programming-In-Python/tree/c42be849d3404d752c68046de86ce53e8d482e4b/SBDL%20-%20Starter).