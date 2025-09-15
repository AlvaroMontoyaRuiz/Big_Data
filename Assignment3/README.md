# SETUP and Execution Instructions

1) Made use of the docker compose file provided.
2) Build the image and run containers to navigate to pgadmin.
3) Make sure the following libraries are installed

    pip install psycopg2-binary python-dateutil 
    conda install psycopg2 python-dateutil

# Execution Instructions

1) performance_analysis.py: Please use the command python performance_analysis.py
2) Partition_solution.py : Please use the following command to run a new data migration process
python partition_solution.py --clean  

Please use the following command to resume a data migration process already executed before
python partition_solution.py 

There is an option to run delete queries. if they are executed you need to re run partition_solution.py without the delete operations in order to get output from the optimization_report.py

3) optimization_report.py: Please make sure to run partition_solution.py before running the optimziation.py as this is a report generation based on tables and migrated data executed on partition_solution

the command to execute the report is python optimization_report.py

4) deployment_script.py: Please run the command python deployment_script.py --dry-run if a simulated run is needed and python deployment_script.py for a full deployment

**The global configuration is located on the top part of the python files, where NUM_DAYS AND EVENTS_PER_DAY can be modified.**

Test metrics outputs have been saves into respective txt files