Techonology used:
Python
    import csv to read csv files
    import json to convert output into json string format
    import flask to create webui for uploading csv files and displaying json string

Pytest
    To run all test files and test suites

Installation and setup instructions:
Note: Make sure that the current directory is in csv_converter
1. If you don't have pyenv installed, execute the installation script
    bash install.sh
2. Create a virtual env using script 
    bash create_venv.sh
3. Activate to your VENV
    pyenv activate py_3_8_12
4. Install required packages
    pip install -r requirements.txt

Running tests:  Note: Make sure that the current directory is in csv_converter
1. Run pytest
    py.test

Running the app:    Note: Make sure that the current directory is in csv_converter
1. Run main.py
    python3 main.py

Logic of handle:    Note: Assumption that csv file is valid
The input CSV will look like

  | Base url  | 1-id | 1-name | 1-url | 2-id | 2-name | 2-url | 3-id | 3-name | 3-url | 
  |:---------:|:----:|:------:|:-----:|:----:|:------:|:-----:|:----:|:------:|:-----:|
  | "/browse" |    1 |   Best |  ...  |    2 |  Fresh |  ...  |    4 | Cheese |  ...  | 
  | "/browse" |    1 |   Best |  ...  |    2 |  Fresh |  ...  |    5 |  Soups |  ...  | 
  | "/browse" |    1 |   Best |  ...  |    2 |  Fresh |  ...  |    6 |  Pizza |  ...  | 
  | "/browse" |    1 |   Best |  ...  |    3 | Drinks |  ...  |    7 |   Wine |  ...  |
  | "/browse" |    1 |   Best |  ...  |    3 | Drinks |  ...  |    8 |   Beer |  ...  |
  


It will then be converted to a dictionary with level as keys, and rows with values from columns specific to that level

  Level 1
  | key[id] | name | id | url |
  |:-------:|:----:|:--:|----:|
  |       1 | Best |  1 | ... |

  Level 2
  | key[(pid, id)] |   name | id | url |
  |:--------------:|:------:|:--:|:---:|
  |         (1, 2) |  Fresh |  2 | ... |
  |         (1, 3) | Drinks |  3 | ... |

  Level 3
  | key[(pid, id)] |   name | id | url | 
  |:--------------:|-------:|:--:|:---:| 
  |         (2, 4) | Cheese |  4 | ... |
  |         (2, 5) |  Soups |  5 | ... | 
  |         (2, 6) |  Pizza |  6 | ... |
  |         (3, 7) |   Wine |  7 | ... |
  |         (3, 8) |   Beer |  8 | ... |


It will then be combined to a single node. Rows having common parent will be aggregated together

  Sample aggregate
  | key[pid] |                     children                       |
  |:--------:|:--------------------------------------------------:|
  |        3 | [{"name":Wine, "id": 7, "url":..., "children":[]}, {"name":Beer, "id": 8, "url":..., "children":[]}] |

  End node
  | name | id | url |                       children                       |
  |:----:|:--:|:---:|:----------------------------------------------------:|
  | Best |  1 | ... | {"name":Drinks, "id":3, "url":..., "children":[{"name":Wine, "id":7, "url"..., "children"[]}, {"name":Beer, "id":8, "url"..., "children"[]}], ... |

