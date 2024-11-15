import sys
import psycopg
import credentials_proj
from loading_helper_functions import (ingest_data, conn_cur,
                                      load_small_table_scorecard)

dataset_filename = sys.argv[1]
dict_filename = 'col_scor_dict.xlsx'
sheet_name = "Institution_Data_Dictionary"
dict_columns = ["VARIABLE NAME", "VALUE", "LABEL"]

col_scor_df, col_scor_dict_df = ingest_data(dataset_filename, dict_filename,
                                            sheet_name, dict_columns)

host = "pinniped.postgres.database.azure.com"
dbname = "atshih"
credentials_module = credentials_proj

conn, cur = conn_cur(host, dbname, credentials_module)

num_rows_inserted = 0

with conn.transaction():

    # preddegs
    load_small_table_scorecard(cur, conn,
                               small_table_name='preddegs',
                               df_to_filter=col_scor_dict_df,
                               df_to_filter_var_name='VARIABLE NAME',
                               df_to_filter_var_val='PREDDEG',
                               id_col='VALUE',
                               value_col='LABEL',
                               small_tbl_id_col='preddeg_id',
                               small_tbl_val_col='preddeg')

    # highdegs
    load_small_table_scorecard(cur, conn,
                               small_table_name='highdegs',
                               df_to_filter=col_scor_dict_df,
                               df_to_filter_var_name='VARIABLE NAME',
                               df_to_filter_var_val='HIGHDEG',
                               id_col='VALUE',
                               value_col='LABEL',
                               small_tbl_id_col='highdeg_id',
                               small_tbl_val_col='highdeg')

    # accred_agencies
    load_small_table_scorecard(cur, conn,
                               small_table_name='accred_agencies',
                               df_to_filter=col_scor_df,
                               df_to_filter_var_name='',
                               df_to_filter_var_val='ACCREDAGENCY',
                               id_col='',
                               value_col='',
                               small_tbl_id_col='agency_id',
                               small_tbl_val_col='accred_agency')

# institutions_non_static
try:
    cur.execute("SELECT * FROM accred_agencies")
    agency_mapping = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("TRUNCATE TABLE institutions_non_static CASCADE")
    cur.executemany("""
        INSERT INTO institutions_non_static (unit_id,
                    year,
                    agency_id,
                    preddeg_id,
                    highdeg_id,
                    adm_rate,
                    tuitionfee_in,
                    tuitionfee_out,
                    tuitionfee_prog,
                    tuitfte,
                    avgfacsal,
                    cdr2,
                    cdr3,
                    actcmmid,
                    actcm25,
                    actcm75)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            (None,
                str(dataset_filename[9:13] + "-" + dataset_filename[14:16]),
                agency_mapping.get(row['ACCREDAGENCY'], None),
                row['PREDDEG'],
                row['HIGHDEG'],
                row['ADM_RATE'],
                row['TUITIONFEE_IN'],
                row['TUITIONFEE_OUT'],
                row['TUITIONFEE_PROG'],
                row['TUITFTE'],
                row['AVGFACSAL'],
                row['CDR2'],
                row['CDR3'],
                row['ACTCMMID'],
                row['ACTCM25'],
                row['ACTCM75']) for i,
            row in col_scor_df.iterrows()
            ]
    )
    print("institutions_non_static now contains data from",
          dataset_filename[9:13], "-", dataset_filename[14:16])
except psycopg.errors.ForeignKeyViolation as e:
    print("""An entry has a unit_id that doesn't
        exist in the static table:""", e)
    conn.rollback()
except psycopg.errors.UniqueViolation as e:
    print("Attempted to insert a duplicate key value:", e)
    conn.rollback()
except psycopg.errors.CheckViolation as e:
    print("A new row violates a check constraint:", e)
    conn.rollback()
except psycopg.errors.NumericValueOutOfRange as e:
    print("An integer passed in is out of range:", e)
    conn.rollback()
except Exception as e:
    print("An error has occurred:", e)
    conn.rollback()
else:
    num_rows_inserted += len(col_scor_df)

conn.commit()
print("Number of rows inserted into tables", num_rows_inserted)

cur.close()
conn.close()
