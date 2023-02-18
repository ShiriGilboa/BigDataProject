import sys
import os
import papermill as pm
from ingestion_pipeline import IngestionPipeLineManager
from sql_db_manager import get_sql_con

DEFAULT_DB_NAME = "NBA_DATA_BIG_DATA_PROJECT.db"
analysis_path = "data_analysis"
ANALYSIS_JUPYTER_NOTEBOOK = "Big_data_Final_project_analysis.ipynb"
db_connection = None
pipeline_manager = None


def is_valid_choice(operation: str):
    return operation.isdigit() and int(operation) in range(len(operations))


def create_new_db():
    print("Default DB name is {}".format(DEFAULT_DB_NAME))
    global db_connection
    if db_connection:
        try:
            db_connection.close()
        except Exception as ex:
            print("couldn't close the connection")
    new_db_connection = get_sql_con(db_name=DEFAULT_DB_NAME, db_from_scratch=True)
    db_connection = new_db_connection
    # If we are starting new DB, must create new Pipeline manager!!
    global pipeline_manager
    pipeline_manager = None
    return db_connection


def build_update_db():
    global db_connection
    db_connection = get_sql_con(DEFAULT_DB_NAME)
    global pipeline_manager
    if not pipeline_manager:
        pipeline_manager = IngestionPipeLineManager(db_connection)
    else:
        pipeline_manager.update_db_connection(db_connection)
    pipeline_manager.run()


def run_jupyter_notebook():
    print("Going to run Jupyer notebook {}".format(os.path.join(analysis_path, ANALYSIS_JUPYTER_NOTEBOOK)))
    print("Output notebook will be in {}".format(os.path.join(analysis_path, "results_{}".format(ANALYSIS_JUPYTER_NOTEBOOK))))
    print("It might take a while to run all analysis..")
    pm.execute_notebook(input_path=os.path.join(analysis_path, ANALYSIS_JUPYTER_NOTEBOOK),
                        output_path=os.path.join(analysis_path, "results_{}".format(ANALYSIS_JUPYTER_NOTEBOOK)),
                        cwd=os.path.join(os.getcwd(), analysis_path), stdout_file=sys.stdout)


def terminate_program():
    print("That's it for now, thank you !")
    exit(1)


operations = {"Create New DB from scratch": create_new_db,
              "Update Your DB from NBA feed": build_update_db,
              "Run Data analysis": run_jupyter_notebook,
              "terminate": terminate_program}

print("Welcome to live NBA Feed!")
while True:
    print("When do you want to do ?")
    for operation in enumerate(operations.keys()):
        print(operation)
    oper = input("Please choose valid option {} \n".format(range(len(operations) - 1)))
    while not is_valid_choice(oper):
        print("{} is not good choice Please choose valid option (0-{})".format(oper, len(operations) - 1))
        oper = input("So.. What do like to do ? \n")
    list(operations.values())[int(oper)]()
