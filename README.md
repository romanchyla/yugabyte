## Getting Started

Utility to exercise yugabyte (distributed database) to test some use-cases that I care about.

## Installing
### Prerequisites
* Yugabyte
* Python3
* sqlalchemy
## Install the Python requirements

Use the pip command to install all
of the Python dependencies listed in the requrements.txt file

```
pip3 install -r requirements.txt
```
This environment is now configured to run the python code.  

### Open `local_config.py` and update settings:
```
SQLALCHEMY_URL = ''
```
---
### Ingest data (logs are written into ./logs)
```
python3 run.py -i <key/value file>
```
