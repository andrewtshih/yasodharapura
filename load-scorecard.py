import sys
import pandas as pd
import numpy as np
import psycopg
import credentials_proj

filename = sys.argv[1]

# Load the College Scorecard dataset passed in as a command argument as a csv
col_scor_df = pd.read_csv(filename)
# Load the College Scorecard data dictionary
col_scor_dict_df = (pd.read_excel(
    'col_scor_dict.xlsx',
    sheet_name="Institution_Data_Dictionary")[["VARIABLE NAME",
                                               "VALUE",
                                               "LABEL"]])

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com",
    dbname="atshih",
    user=credentials_proj.DB_USER,
    password=credentials_proj.DB_PASSWORD
)

cur = conn.cursor()

num_rows_inserted = 0

# make a new transaction
with conn.transaction():

    # preddegs
    try:
        cur.execute("TRUNCATE TABLE preddegs CASCADE")
        preddegs = (col_scor_dict_df[col_scor_dict_df['VARIABLE NAME']
                                     == 'PREDDEG'])
        cur.executemany("""
            INSERT INTO preddegs (preddeg_id, preddeg)
            VALUES (%s, %s)
            """, [
                (row['VALUE'], row['LABEL']) for i,
                row in preddegs.iterrows()
                ]
        )
        print("preddegs now contains data from",
              filename[9:13], "-", filename[14:16])
    except Exception as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()

    # highdegs
    try:
        cur.execute("TRUNCATE TABLE highdegs CASCADE")
        highdegs = (col_scor_dict_df[col_scor_dict_df['VARIABLE NAME']
                                     == 'HIGHDEG'])
        cur.executemany("""
            INSERT INTO highdegs (highdeg_id, highdeg)
            VALUES (%s, %s)
            """, [
                (row['VALUE'], row['LABEL']) for i,
                row in highdegs.iterrows()
                ]
        )
        print("highdegs now contains data from",
              filename[9:13], "-", filename[14:16])
    except Exception as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()

    # accred_agencies
    try:
        cur.execute("TRUNCATE TABLE accred_agencies CASCADE")
        accred_agencies = (col_scor_df[['ACCREDAGENCY']].drop_duplicates().
                           reset_index(drop=True))
        cur.executemany("""
            INSERT INTO accred_agencies (agency_id, accred_agency)
            VALUES (%s, %s)
            """, [
                (i + 1, row['ACCREDAGENCY']) for i,
                row in accred_agencies.iterrows()
                ]
        )
        print("accred_agencies now contains data from",
              filename[9:13], "-", filename[14:16])
    except Exception as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()

    # institutions_non_static
    try:
        cur.execute("SELECT * FROM accred_agencies")
        agency_mapping = {row[1]: row[0] for row in cur.fetchall()}

        col_scor_df = col_scor_df.replace(np.nan, None)

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
                 str(filename[9:13] + "-" + filename[14:16]),
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
                 row['ACTCM75']
                 ) for i,
                row in col_scor_df.iterrows()
                ]
        )
        print("institutions_non_static now contains data from",
              filename[9:13], "-", filename[14:16])
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
        # no exception happened, so we continue without reverting the
        # savepoint
        num_rows_inserted += len(col_scor_df)

# now we commit the entire transaction
conn.commit()
print("Number of rows inserted into tables", num_rows_inserted)

cur.close()
conn.close()
