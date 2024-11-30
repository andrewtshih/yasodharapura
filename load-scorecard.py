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
dbname = credentials_proj.dbname
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

    # The accred_agencies table is constructed very differently,
    # so values are inserted without using a helper function.
    try:
        acc_agencies = (col_scor_df[['ACCREDAGENCY']].drop_duplicates()
                        .reset_index())
        cur.executemany("""
                        INSERT INTO accred_agencies (agency_id, accred_agency)
                        VALUES (%s, %s)
                        ON CONFLICT (agency_id) DO UPDATE
                            SET agency_id = EXCLUDED.agency_id,
                                accred_agency = EXCLUDED.accred_agency
                        """,
                        [(i + 1, row['ACCREDAGENCY']) for i,
                         row in acc_agencies.iterrows()])
    except psycopg.errors.UniqueViolation as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()
    except Exception as e:
        print("An error has occurred:", e)
        conn.rollback()

    # The institutions_non_static table is constructed very differently,
    # so values are inserted without using a helper function.
    try:
        cur.execute("SELECT * FROM accred_agencies")
        accred_agencies_mapping = {row[1]: row[0] for row in cur.fetchall()}

        current_row_index = 0

        for i, row in col_scor_df.iterrows():
            current_row_index = i
            print("Loading row " + str(current_row_index + 1))
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
                ON CONFLICT (unit_id, year) DO NOTHING """
                        #     UPDATE
                        # SET year = EXCLUDED.year,
                        #     agency_id = EXCLUDED.agency_id,
                        #     preddeg_id = EXCLUDED.preddeg_id,
                        #     highdeg_id = EXCLUDED.highdeg_id,
                        #     adm_rate = EXCLUDED.adm_rate,
                        #     tuitionfee_in = EXCLUDED.tuitionfee_in,
                        #     tuitionfee_out = EXCLUDED.tuitionfee_out,
                        #     tuitionfee_prog = EXCLUDED.tuitionfee_prog,
                        #     tuitfte = EXCLUDED.tuitfte,
                        #     avgfacsal = EXCLUDED.avgfacsal,
                        #     cdr2 = EXCLUDED.cdr2,
                        #     cdr3 = EXCLUDED.cdr3,
                        #     actcmmid = EXCLUDED.actcmmid,
                        #     actcm25 = EXCLUDED.actcm25,
                        #     actcm75 = EXCLUDED.actcm75
                # """
                , [
                    (row['UNITID'],
                        str(dataset_filename[9:13] + "-" +
                            dataset_filename[14:16]),
                        accred_agencies_mapping.get(row['ACCREDAGENCY'], None),
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
                        row['ACTCM75'])]
            )
        # print("institutions_non_static now contains data from",
        #       dataset_filename[9:13], "-", dataset_filename[14:16])
    except psycopg.errors.ForeignKeyViolation as e:
        print(f"""Row {current_row_index + 1} has a unit_id that doesn't
            exist in the static table:""", e)
        conn.rollback()
    except psycopg.errors.UniqueViolation as e:
        print(f"""Attempted to insert a duplicate key value at
              row {current_row_index + 1}:""", e)
        conn.rollback()
    except psycopg.errors.CheckViolation as e:
        print(f"Row {current_row_index + 1} violates a check constraint:", e)
        conn.rollback()
    except psycopg.errors.NumericValueOutOfRange as e:
        print(f"An integer in row {current_row_index + 1} is out of range:", e)
        conn.rollback()
    except Exception as e:
        print("An error has occurred:", e)
        conn.rollback()
    else:
        num_rows_inserted += len(col_scor_df)

conn.commit()
print("Number of rows inserted into institutions_non_static:",
      num_rows_inserted)

cur.close()
conn.close()
