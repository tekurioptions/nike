# Nike Test

## Overview

This is a samll assignment as a part of interview process

## Installing dependencies

#### Install with Conda Environments

Create the environment:
```
conda create -n nike-test python=3.7
conda activate nike-test
```

Install requirements with pip:
```
pip install -r src/requirements.txt
```

Make sure you select your new environment as the project interpreter in PyCharm.

## Configurations
please replace the 'path' with appropriate s3 bucket details in the pipeline.py file

```
s3a://[path]
```

## Running the pipeline 

first clone the repo to local

```
git clone https://github.com/tekurioptions/nike.git
$ cd {PROJECT_ROOT_DIRECTORY}
```

to run for particular year and week

```
spark-submit src/pipeline.py [year] [week]
example: spark-submit src/pipeline.py 2018 2
```

to run for all years and weeks

```
spark-submit src/pipeline.py
example: spark-submit src/pipeline.py
```
