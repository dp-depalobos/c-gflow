Techonology used:
Python PySpark
    Supports consuming data in .csv format and writing output in .json format
    Built in aggregate functions to combine rows under the same parent object
Unittest
    Framework for writing scalable test cases in python
Pandas testing framework
    Built in function to compare two dataframe objects

Installation and setup instructions:
1. Install pyenv on the current environment as per instructions here:
    https://github.com/pyenv/pyenv
2. Install any 3.8 version of python.
    pyenv install 3.8.10 #you can replace this version with any 3.8.*
3. Create python virtualenv
    pyenv virtualenv 3.8.10 py_3_8_10 #you can replace the venv name here
4. Activate virtualenv
    pyenv activate py_3_8_10
5. Install all dependencies in requirements.txt
    pip install -r requirements.txt
6. Check if all packages have been installed
    pip freeze > packages.txt #compare the contents from requirements.txt and packages.txt

Instructions on how to run test
1. Inside virtualenv and folder which contains test_*.py run command py.test

Instructions on how to run script    
1. Replace the input_path for the .csv data to consume
2. Replace the output_path for where to write the .json result

Things to improve on:
1. Improvement in time complexity in for loops
2. Check if there could be more things to refactor
3. Fix logic where parents with no child are set to null instead of array
4. Handle duplicate data
5. Create error message when there are problems in the input csv