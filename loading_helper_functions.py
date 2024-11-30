import pandas as pd
import numpy as np
import psycopg


def ingest_data(dataset_filename: str, dict_filename: str,
                sheet_name: str, dict_columns: list, encoding='utf-8'):
    """Read in dataset and data dictionary, and handle missing data."""

    dataset_df = pd.read_csv(dataset_filename, encoding=encoding)

    dict_df = pd.read_excel(dict_filename,
                            sheet_name=sheet_name)[dict_columns]

    dataset_df = dataset_df.replace(np.nan, None)

    return dataset_df, dict_df


def conn_cur(host: str, dbname: str, credentials_module):
    """Create connection and cursor for SQL server."""
    conn = psycopg.connect(
        host=host,
        dbname=dbname,
        user=credentials_module.DB_USER,
        password=credentials_module.DB_PASSWORD
    )

    cur = conn.cursor()

    return conn, cur


def load_small_table_scorecard(cur, conn, small_table_name: str,
                               df_to_filter: pd.DataFrame,
                               df_to_filter_var_name: str,
                               df_to_filter_var_val: str,
                               id_col: str, value_col: str,
                               small_tbl_id_col: str, small_tbl_val_col: str):
    """
    Load College Scorecard data in to dim tables. The data can either come from
    the actual data from the U.S. DOE or from the data dictionary.

    Parameters
    ----------
    cur: cursor
    conn: connection
    small_table_name: name of dim table
    df_to_filter: name of data dictionary/dataset dataframe
    df_to_filter_var_name: name of column in data dictionary/dataset dataframe
    df_to_filter_var_val: name of value in data dictionary/dataset dataframe
    id_col: name of id column in data dictionary/dataset dataframe
    value_col: name of value column in data dictionary/dataset dataframe
    small_tbl_id_col: name of dim table id column
    small_tbl_val_col: name of dim table value column
    """

    try:
        if small_table_name == 'accred_agencies':
            filtered_df = (df_to_filter[[df_to_filter_var_val]]
                           .drop_duplicates().reset_index())
            to_insert = [
                (i + 1, row[df_to_filter_var_val]) for i,
                row in filtered_df.iterrows()
            ]
        else:
            filtered_df = (df_to_filter[df_to_filter[df_to_filter_var_name]
                                        == df_to_filter_var_val])
            to_insert = [
                (row[id_col], row[value_col]) for i,
                row in filtered_df.iterrows()
                ]

        cur.executemany(
            f"""INSERT INTO {small_table_name} ({small_tbl_id_col},
                {small_tbl_val_col})
                VALUES (%s, %s)
                ON CONFLICT ({small_tbl_id_col}) DO UPDATE
                    SET {small_tbl_val_col} = EXCLUDED.{small_tbl_val_col}""",
            to_insert
        )
    except psycopg.errors.UniqueViolation as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()
    except Exception as e:
        print("An error has occurred:", e)
        conn.rollback()


def load_small_table_ipeds(cur, conn, small_table_name: str,
                           df_to_filter: pd.DataFrame,
                           df_to_filter_var_name: str,
                           df_to_filter_var_val: str,
                           id_col: str, value_col: str,
                           null_exists: bool, null_condition: str,
                           small_tbl_id_col: str, small_tbl_val_col: str):
    """
    Load IPEDS data in to dim tables. The data can either come from
    the actual data from the U.S. DOE or from the data dictionary.

    Parameters
    ----------
    cur: cursor
    conn: connection
    small_table_name: name of dim table
    df_to_filter: name of data dictionary/dataset dataframe
    df_to_filter_var_name: name of column in data dictionary/dataset dataframe
    df_to_filter_var_val: name of value in data dictionary/dataset dataframe
    id_col: name of id column in data dictionary/dataset dataframe
    value_col: name of value column in data dictionary/dataset dataframe
    null_exists: boolean to indicate whether there is a category
        for N/A entries
    null_condition: string that indicates an N/A entry for this categorization
    small_tbl_id_col: name of dim table id column
    small_tbl_val_col: name of dim table value column
    """

    try:
        filtered_df = (df_to_filter[df_to_filter[df_to_filter_var_name]
                                    == df_to_filter_var_val])
        if null_exists:
            to_insert = [
                (row[id_col], None) if row[id_col] == null_condition else
                (row[id_col], row[value_col]) for i,
                row in filtered_df.iterrows()
                ]
        else:
            to_insert = [(row[id_col], row[value_col]) for i,
                         row in filtered_df.iterrows()]

        cur.executemany(
            f"""INSERT INTO {small_table_name} ({small_tbl_id_col},
                {small_tbl_val_col})
                VALUES (%s, %s)
                ON CONFLICT ({small_tbl_id_col}) DO UPDATE
                    SET {small_tbl_val_col} = EXCLUDED.{small_tbl_val_col}""",
            to_insert
        )
    except psycopg.errors.UniqueViolation as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()
    except Exception as e:
        print("An error has occurred:", e)
        conn.rollback()
