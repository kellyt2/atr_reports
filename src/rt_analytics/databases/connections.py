import os
from datetime import datetime
from typing import List, Union
import cx_Oracle
import pandas as pd
import psycopg2
import pyodbc


def check_environment_credentials(database: str = "Redshift AD") -> None:
    """
    Check that the necessary database credentials are stored as enviroment variables.
    
    Args:
        database (str, optional): Name of databses to connect to. Defaults to "Redshift AD".
    
    Raises:
        ValueError: The inputted database is not one of the allowed databases.
        KeyError: Some of the necessary credentials are missing.
    
    Returns:
        None
    """
    allowed_databases = ["Redshift AD", "Redshift", "POD", "DW"]
    required_credentials = ["USERNAME", "PASSWORD"]

    # check database in allowed list
    if database not in allowed_databases:
        raise ValueError("database must be one of " + ", ".join(allowed_databases))

    if database == "Redshift AD":
        try:
            conn = pyodbc.connect("DSN=Redshift ODBC")
            conn.close()
        except:
            raise KeyError("DNS Redshift ODBC is missing from system.")
    else:
        # check that correct enviroment credentials exist
        credentials_to_test = [database.upper() + "_" + s for s in required_credentials]

        for c in credentials_to_test:
            if c not in os.environ:
                raise KeyError(
                    "{0:s} variable is missing from environment and/or .env file.".format(
                        c
                    )
                )
    return


# Database connections
def open_connection(
    database: str = "Redshift AD", db_creds: dict = None
) -> Union[pyodbc.Connection, psycopg2.extensions.connection]:  # type: ignore

    """
    Open a connection to the server
    
    Args:
        database (str, optional): Name of database to connect to. Defaults to "Redshift AD".
        db_creds (dict, optional): Credentials to be used to override environment
            credentials. Defaults to None.

    Raises:
        ValueError: An error occured passing through an unsupported database. 

    Returns:
        Union[pyodbc.Connection, psycopg2.extensions.connection] : Connection object.  
    """

    # check if credentials are passed through into function
    if db_creds is not None:
        username = db_creds.get("username", None)
        password = db_creds.get("password", None)
    else:
        check_environment_credentials(database="Redshift AD")

        username = os.getenv("{0}_USERNAME".format(database.upper()))
        password = os.getenv("{0}_PASSWORD".format(database.upper()))

    # configure databases
    if database == "Redshift AD":
        config = {}
    elif database == "Redshift":
        config = {
            "dbname": "edcrsdbprod",
            "user": username,
            "pwd": password,
            "host": "redshift.app.betfair",
            "port": "5439",
        }
    elif database == "POD":
        config = {
            "dbname": "ods",
            "user": username,
            "pwd": password,
            "host": "prdpod001.prd.betfair",
            "port": "5432",
        }
    elif database == "DW":
        config = {
            "dbname": "DWQUERY_USERS",
            "user": username,
            "pwd": password,
            "host": "dw-db-scan-vip.prd.betfair",
        }
    else:
        raise ValueError("Database {0} is not of configured type.".format(database))

    # create connection
    if database == "Redshift AD":
        conn = pyodbc.connect("DSN=Redshift ODBC")
        print("Connection to {0} Successful".format(database))
    elif database in ("Redshift", "POD"):
        conn = psycopg2.connect(
            dbname=config["dbname"],
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["pwd"],
            sslmode="require",
        )
        print("Connection to {0} Successful".format(database))
    elif database == "DW":
        conn_string = config["user"] + "/" + config["pwd"] + "@" + config["host"]
        conn = cx_Oracle.connect(conn_string)
        print("Connection to DW Successful")
    else:
        raise ValueError("Database is not of configured type.")

    return conn


# SQL functions
def run_sql_string(cur, sql_string: str) -> dict:
    """
    Run an inputted SQL string as code.
    
    Args:
        cur ([type]): Cursor to run command through.
        sql_string (str): SQL string to be executed.
    
    Returns:
        dict: Results of SQL query.
    """
    results = {}
    results["headers"] = []
    results["data"] = []

    try:
        print("{0:s} Running SQL Query: ...".format(str(datetime.now())), end="")

        # execute SQL code, triple quotes allow easy single quotes
        cur.execute(sql_string)
        print("DONE")

        # put output into results dict
        results["headers"] = cur.description

        if results["headers"] is not None:
            results["data"] = cur.fetchall()

    except Exception as ex:
        print("Error in query")
        template = "\tAn exception of type {0} occured. Arguments:\n\t{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    return results


def run_sql_file(cur, sql_file_name: str) -> dict:
    """
    Run an inputted SQL string as code.
    
    Args:
        cur ([type]): Cursor to run command through.
        sql_file_name (str): Location of SQL file to be executed.
    
    Returns:
        dict: Results of SQL query.
    """
    results = {}
    results["headers"] = []
    results["data"] = []

    try:
        f = open(sql_file_name, "r")

        # read in query
        sql_query = f.read()

        print(
            "{0:s} File: {1:s} ...".format(
                str(datetime.now()), os.path.basename(sql_file_name)
            ),
            end="",
        )

        # execute SQL code, triple quotes allow easy single quotes
        cur.execute(sql_query)
        print("DONE")

        # put output into results dict
        results["headers"] = cur.description
        if results["headers"] is not None:
            results["data"] = cur.fetchall()

        f.close()

    except Exception as ex:
        print("Error in file {0:s}".format(sql_file_name))
        template = "\tAn exception of type {0} occured. Arguments:\n\t{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)

    return results


def print_sql_results(sql_results: dict) -> None:
    """
    Print output of a SQL query to screen.
    
    Args:
        sql_results (dict): Output of query
    """

    headers = sql_results["headers"]
    data = sql_results["data"]

    # print column headers
    print(",".join(str(e[0]) for e in headers))

    # print row from data
    for row in data:
        print(",".join(str(e) for e in row))

    return


def print_sql_results_to_csv(sql_results: dict, csv_file: str) -> None:
    """
    Print output of a SQL query to a CSV file.
    
    Args:
        sql_results (dict): Output of query
        csv_file (str): Location of CSV to write results into.
    """
    headers = sql_results["headers"]
    data = sql_results["data"]

    # open csv
    f = open(csv_file, "w", encoding="utf-8")
    # print header
    f.write("{0}\n".format(",".join(str(e[0]) for e in headers)))

    for row in data:
        f.write("{0}\n".format(",".join(str(e) for e in row)))
    f.close()

    return


def run_sql_file_to_csv(cur, sql_file_name: str, csv_file: str):
    """
    Execute a SQL file and save output as a CSV file.
    
    Args:
        cur ([type]): cursor connected to active database.
        sql_file_name (str): Location of the SQL file to be executed.
        csv_file (str): Location of CSV to write results into.
    """

    sql_results = run_sql_file(cur, sql_file_name)

    print_sql_results_to_csv(sql_results, csv_file)

    return


def run_sql_scripts(
    sql_scripts: Union[str, List[str]],
    csv_output: bool = False,
    csv_dir: str = None,
    database: str = "Redshift AD",
    db_cred: dict = None,
) -> Union[str, List[str]]:
    """
    Script to run a single or multiple script
    
    Args:
        sql_scripts (Union[str, List[str]]): Location of sql files to run.
        csv_output (bool, optional): Parameter to determine if SQL output is to be saved in a CSV file. Defaults to False.
        csv_dir (str, optional): Name of directory to save CSV files into. Defaults to None.
        datbase (str, optional): Database to run SQL against. Defaults to "RedshiftcAD".
        db_cred (dict, optional): Credentials for database that will overwrite environmental credentials. Defaults to None.
    
    Returns:
        Union[str, List[str]]: Outputted filenames of CSV created (if any).
    """

    csv_files = []

    # check sql_scripts is of list type, if str convert to list
    if type(sql_scripts) is str:
        sql_scripts = [sql_scripts]

    # open a Redshift Connection
    conn = open_connection(database=database, db_creds=db_cred)

    # loop over sql_scripts
    for sql_file_path in sql_scripts:
        if csv_output == True:
            # write results to CSV
            if csv_dir is None:
                # No CSV directory specified, use folder of source file
                csv_file_name = sql_file_path.replace(".sql", ".csv")
            else:
                csv_dir_clean = _assure_path_exists(csv_dir)
                sql_file_name = os.path.basename(sql_file_path)
                csv_file_name = csv_dir_clean + sql_file_name.replace(".sql", ".csv")

            run_sql_file_to_csv(conn.cursor(), sql_file_path, csv_file_name)
            csv_files.append(csv_file_name)
        else:
            run_sql_file(conn.cursor(), sql_file_path)
            pass

    # close connection
    conn.cursor().close()
    conn.close()

    return csv_files


def run_sql_scripts_df(
    sql_scripts: Union[str, List[str]],
    database: str = "Redshift AD",
    db_cred: dict = None,
) -> pd.DataFrame:
    """
    Run a single or multiple script on a database and store results in a DataFrame.
    
    Args:
        sql_scripts (Union[str, List[str]]): Location of sql files to run.
        datbase (str, optional): Database to run SQL against. Defaults to "Redshift AD".
        db_cred (dict, optional): Credentials for database that will overwrite environmental credentials.. Defaults to None.
    
    Returns:
        pd.DataFrame: Outputted DataFrame from SQL outputs
    """

    # initialise returned DataFrame
    df_out = pd.DataFrame([])
    temp_df = pd.DataFrame([])

    # check sql_scripts is of list type, if str convert to list
    if type(sql_scripts) is str:
        sql_scripts = [sql_scripts]

    # open a Redshift Connection
    conn = open_connection(database=database, db_creds=db_cred)

    # loop over sql_scripts
    for sql_filename in sql_scripts:
        try:
            f = open(sql_filename, "r")

            # read in query
            sql_query = f.read()

            print(
                "{0:s} File: {1:s} ...".format(
                    str(datetime.now()), os.path.basename(sql_filename)
                ),
                end="",
            )

            temp_df = pd.read_sql(sql_query, conn)

            print("DONE")

            # append to returned DataFrame
            df_out = df_out.append(temp_df, ignore_index=True)

            # close file
            f.close()

        except Exception as ex:
            print("Error in file {0:s}".format(sql_filename))
            template = "\tAn exception of type {0} occured. Arguments:\n\t{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            # run sql file

    # close connection
    conn.cursor().close()
    conn.close()

    return df_out


def run_sql_code_df(
    sql_code: Union[str, List[str]],
    database: str = "Redshift AD",
    db_cred: dict = None,
) -> pd.DataFrame:
    """
    Run SQL code on a database and store results in a DataFrame.
    
    Args:
        sql_code (Union[str, List[str]]): SQL code to execute.
        datbase (str, optional): Database to run SQL against. Defaults to "Redshift AD".
        db_cred (dict, optional): Credentials for database that will overwrite environmental credentials.. Defaults to None.
    
    Returns:
        pd.DataFrame: Outputted DataFrame from SQL outputs.
    """

    # initialise returned DataFrame
    df_out = pd.DataFrame([])
    temp_df = pd.DataFrame([])

    # check sql_scripts is of list type, if str convert to list
    if type(sql_code) is str:
        sql_code = [sql_code]

    # open a Redshift Connection
    conn = open_connection(database=database, db_creds=db_cred)

    # loop over sql_scripts
    for sql_string in sql_code:
        try:
            print("{0:s} Running SQL Query: ...".format(str(datetime.now())), end="")

            # create temp DataFrame to put results in
            temp_df = pd.read_sql(sql_string, conn)

            print("DONE")

            # append to returned DataFrame
            df_out = df_out.append(temp_df, ignore_index=True)

        except Exception as ex:
            print("Error in query")
            template = "\tAn exception of type {0} occured. Arguments:\n\t{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)

    # close connection
    conn.cursor().close()
    conn.close()

    return df_out


# Utility functions
def _assure_path_exists(path: str) -> str:
    """
    Checks if directory exists, if not it creates it.
    
    Args:
        path (str): path to directory.
    
    Returns:
        str: path to directory that now definitely exists.
    """
    clean_path = path
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)

    if path[-1] != "/":
        clean_path = path + "/"

    return clean_path
