import sys
import psycopg
import credentials_proj
# import numpy as np
from loading_helper_functions import (ingest_data, conn_cur,
                                      load_small_table_ipeds)

dataset_filename = sys.argv[1]
dict_filename = 'ipeds_dict.xlsx'
sheet_name = "Frequencies"
dict_columns = ["varname", "codevalue", "valuelabel"]
encoding = 'unicode_escape'

ipeds_df, ipeds_dict_df = ingest_data(dataset_filename, dict_filename,
                                      sheet_name, dict_columns,
                                      encoding=encoding)

ipeds_df['ZIP'] = ipeds_df['ZIP'].str.slice(0, 5)

# further preprocessing
geo_cols_to_set_to_none = ['COUNTYCD', 'CBSA', 'CBSATYPE', 'CSA']
for col in geo_cols_to_set_to_none:
    ipeds_df[col] = ipeds_df[col].replace(-2, None)

if int(dataset_filename[5:9]) < 2021:
    cc_cols_to_set_to_none = ['C21BASIC', 'C21IPUG', 'C21IPGRD',
                              'C21UGPRF', 'C21ENPRF', 'C21SZSET']
    ipeds_df[cc_cols_to_set_to_none] = None

# connecting to the database and creating a cursor
host = "pinniped.postgres.database.azure.com"
dbname = credentials_proj.dbname
credentials_module = credentials_proj

conn, cur = conn_cur(host, dbname, credentials_module)

num_rows_inserted = 0

# make a new transaction
with conn.transaction():

    load_small_table_ipeds(cur, conn,
                           small_table_name='controls',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='CONTROL',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-3',
                           small_tbl_id_col='control_id',
                           small_tbl_val_col='control')

    # regions
    load_small_table_ipeds(cur, conn,
                           small_table_name='regions',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='OBEREG',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=False,
                           null_condition='',
                           small_tbl_id_col='region_id',
                           small_tbl_val_col='region')

    # ccs_basic
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_basic',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21BASIC',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_basic_id',
                           small_tbl_val_col='cc_basic')

    # ccs_ipug
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_ipug',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21IPUG',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_ipug_id',
                           small_tbl_val_col='cc_ipug')

    # ccs_ipgrd
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_ipgrd',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21IPGRD',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_ipgrd_id',
                           small_tbl_val_col='cc_ipgrd')

    # ccs_ugprf
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_ugprf',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21UGPRF',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_ugprf_id',
                           small_tbl_val_col='cc_ugprf')

    # ccs_enprf
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_enprf',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21ENPRF',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_enprf_id',
                           small_tbl_val_col='cc_enprf')

    # ccs_szset
    load_small_table_ipeds(cur, conn,
                           small_table_name='ccs_szset',
                           df_to_filter=ipeds_dict_df,
                           df_to_filter_var_name='varname',
                           df_to_filter_var_val='C21SZSET',
                           id_col='codevalue',
                           value_col='valuelabel',
                           null_exists=True,
                           null_condition='-2',
                           small_tbl_id_col='cc_szset_id',
                           small_tbl_val_col='cc_szset')

    # The cities table is constructed very differently,
    # so values are inserted without using a helper function.
    try:
        cities = ipeds_df[['CITY', 'STABBR']].drop_duplicates().reset_index()
        cur.executemany("""
                        INSERT INTO cities (city_id, city, stabbr)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (city_id) DO UPDATE
                            SET city_id = EXCLUDED.city_id,
                                city = EXCLUDED.city,
                                stabbr = EXCLUDED.stabbr
                        """,
                        [(i + 1, row['CITY'], row['STABBR']) for i,
                         row in cities.iterrows()])
    except psycopg.errors.UniqueViolation as e:
        print("Attempted to insert a duplicate key value:", e)
        conn.rollback()
    except Exception as e:
        print("An error has occurred:", e)
        conn.rollback()

    # The institutions_static table is constructed very differently,
    # so values are inserted without using a helper function.
    try:
        cur.execute("SELECT * FROM cities")
        cities_mapping = {(row[1], row[2]): row[0] for row in cur.fetchall()}

        # if int(dataset_filename[5:9]) >= 2021:
        current_row_index = 0

        for i, row in ipeds_df.iterrows():
            current_row_index = i
            print("Inserting row " + str(current_row_index + 1))
            cur.executemany("""
                INSERT INTO institutions_static (unit_id,
                            instnm,
                            addr,
                            city_id,
                            zip,
                            county_cd,
                            cbsa,
                            cbsa_type,
                            csa,
                            region_id,
                            latitude,
                            longitude,
                            control_id,
                            c21basic_id,
                            c21ipug_id,
                            c21ipgrd_id,
                            c21ugprf_id,
                            c21enprf_id,
                            c21szset_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (unit_id) DO UPDATE
                        SET instnm = EXCLUDED.instnm,
                            addr = EXCLUDED.addr,
                            city_id = EXCLUDED.city_id,
                            zip = EXCLUDED.zip,
                            county_cd = EXCLUDED.county_cd,
                            cbsa = EXCLUDED.cbsa,
                            cbsa_type = EXCLUDED.cbsa_type,
                            csa = EXCLUDED.csa,
                            region_id = EXCLUDED.region_id,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            control_id = EXCLUDED.control_id,
                            c21basic_id = EXCLUDED.c21basic_id,
                            c21ipug_id = EXCLUDED.c21ipug_id,
                            c21ipgrd_id = EXCLUDED.c21ipgrd_id,
                            c21ugprf_id = EXCLUDED.c21ugprf_id,
                            c21enprf_id = EXCLUDED.c21enprf_id,
                            c21szset_id = EXCLUDED.c21szset_id
                """, [
                    (row['UNITID'],
                        row['INSTNM'],
                        row['ADDR'],
                        cities_mapping.get((row['CITY'], row['STABBR']), None),
                        row['ZIP'],
                        row['COUNTYCD'],
                        row['CBSA'],
                        row['CBSATYPE'],
                        row['CSA'],
                        row['OBEREG'],
                        row['LATITUDE'],
                        row['LONGITUD'],
                        row['CONTROL'],
                        row['C21BASIC'],
                        row['C21IPUG'],
                        row['C21IPGRD'],
                        row['C21UGPRF'],
                        row['C21ENPRF'],
                        row['C21SZSET'])]
            )
        # else:
        #     cur.executemany("""
        #         INSERT INTO institutions_static (unit_id,
        #                     instnm,
        #                     addr,
        #                     city_id,
        #                     zip,
        #                     county_cd,
        #                     cbsa,
        #                     cbsa_type,
        #                     csa,
        #                     region_id,
        #                     latitude,
        #                     longitude,
        #                     control_id)
        #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
        #                 %s, %s, %s, %s)
        #         ON CONFLICT (unit_id) DO UPDATE
        #                 SET value = EXCLUDED.value
        #         """, [
        #             (row['UNITID'],
        #                 row['INSTNM'],
        #                 row['ADDR'],
        #                 cities_mapping.get((row['CITY'], row['STABBR']),
        #                      None),
        #                 row['ZIP'],
        #                 row['COUNTYCD'],
        #                 row['CBSA'],
        #                 row['CBSATYPE'],
        #                 row['CSA'],
        #                 row['OBEREG'],
        #                 row['LATITUDE'],
        #                 row['LONGITUD'],
        #                 row['CONTROL']) for i,
        #             row in ipeds_df.iterrows()
        #             ]
        # )
        # print("2021 Carnegie Classifcation variables do not yet exist.")
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
        num_rows_inserted += len(ipeds_df)

conn.commit()
print("Number of rows inserted into institutions_static:",
      num_rows_inserted)

cur.close()
conn.close()
