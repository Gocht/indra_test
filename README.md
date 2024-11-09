# Indra Test
## Python / PySpark 

Requirements:
- Spark 3.5
- Pandas 2.0+
- Python 3
- spark-xml_2.12-0.18.0.jar (included)
- Data file 'data.xml' must be placed in 'sample' directory

## Structure
```
├── jars
│   ├── spark-xml_1.12-0.18.0.jar
├── sample
│   ├── data.xml
├── output
│   ├── ...
├── pd_output
│   ├── ...
├── main.py
├── pd_main.py
├── out.txt
├── readme.md
└── .gitignore
```

## Files

- `out.txt` file contains the output from `main.py` execution
- `pd_out.txt` file contains the output from `pd_main.py` execution
- `output` directory contains tables creates by pyspark script
- `pd_output` directory contains tables creates by python script

## Run

Located in working directory

To run pyspark script

```sh
spark-submit --jars jars/spark-xml_2.12-0.18.0.jar main.py
```

To run python/pandas script

```sh
python3 pd_main.py
```
