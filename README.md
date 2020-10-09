# Coding task to find the hottest day

## Setup
Assuming a (new) virtual environment is set up already and activated.  

Designed to run with Python 3.8
```
pip install -r requirements.txt
```
Usage:
```
$ python main.py -h
  usage: main.py [-h] [-o OUTPUT] -d DATA
  
  given a weather data set, displays information about the hottest day
  
  optional arguments:
    -h, --help            show this help message and exit
    -o OUTPUT, --output OUTPUT
                          Output src data to parquet format, default: ./weather.parquet
  
  Required named arguments:
    -d DATA, --data DATA  path to csv file containing weather data
```

## Assumptions
I assumptions I had made based on how I understood the task description.
 - convert the raw source data to parquet format (unmodified)
 - worked with Pandas dataframe object to get hottest day information
 - print results of hottest day to terminal
 - 2 data files provided, run 1 at a time (passed as script argument)


## Execute the script
To run for script  
eg.
```
$ python main.py -d ./input/weather.20160201.csv
```
```
$ python main.py -d ./input/weather.20160301.csv
```

## Tests

Unit-tests can be run as:
```
python -m pytest -v tests/test_main.py
```
