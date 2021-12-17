Techonology used:
1. Python PySpark
    - Since it already supports consuming data in .csv format as well as writing output in .json format
    - It has aggregate functions that would help in bundling rows under the same parent object

Instructions on how to use:
1. Replace the input_path for the .csv data to consume
2. Replace the output_path for where to write the .json result

Things to improve on:
(DONE) 1. Create logic for getting heirarchy of records that does not depend on current column names available.
2. Modify logic to automatically iterate to different levels