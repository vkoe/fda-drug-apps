# fda_applications.py
Create SQL database using SQLite and SQLAlchemy in Python, insert data from Pandas dataframes into relational application and application tables, and create a bridge table to facilitate deduplication of applicant companies.

# FDA_apps_dedupe.sql
SQL code to deduplicate redundant applicant names from company table
Specifically:
    - add temporary parent column to company table with corresponding parent id from bridge table
    - delete redundant companies 
    - (bridge table allows for connection to remain between corresponding applications and applicants)
    - remove temporary parent column from company table