# unittesting-for-pyspark
This repository is to demonstrate how you can use Python unittest and pytest libraries to create unit tests for PySpark DataFrame transformations.

# Repository Structure:
* spark_example_proj: Contains the source code.
* tests: Containst the unit test methods using pytest and unittest. Each library contains its own tests inside their subdirectories.
* Other files: gitignore, requirements.txt

# Executing all the unit tests (from the root of the repository)
* pytest (to run all the unit tests (pytests + unittests))
  * `pytest tests`
* unittest (only unittests)
  * `python -m unittest discover -s tests/unittests`

