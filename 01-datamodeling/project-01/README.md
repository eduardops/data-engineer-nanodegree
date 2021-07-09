README.md
...
# Developers
Instructions for installing this project on your local machine and getting it running in its own virtual environment.
1.  Clone this project into a new directory - click the clone button above on GitHub to get the link and command to use
2.  Ensure you have `pipenv` installed on your machine - installation documentation can be found here - https://docs.pipenv.org/en/latest/install/
3.  From the project root directory run `pipenv install --dev --pre` to create your virtual environment and install all the packages
4.  Activate your virtual environment at any time using `pipenv shell` - note only do this from the project directory root
5.  You can now run the program using `python create_tables.py` to create the project tables
5.  You can now run the program using `python etl.py` to migrate data to corresponding tables
6.  Deactivate your virtual environment at any time using `exit`
...