import sys
import pandas as pd
import psycopg
import credentials_proj

filename = sys.argv[1]

# load the actual dataset passed in as a command argument as a csv
ipeds_df = pd.read_csv(filename, encoding='unicode_escape')
# load the ipeds data dictionary
ipeds_dict_df = pd.read_excel('ipeds_dict.xlsx', sheet_name="Frequencies")[["varname", "codevalue", "valuelabel"]]

# # conversions
# zips = ipeds_df['ZIP'].str.slice(0, 5)

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

    # controls
    cur.execute("SELECT COUNT(*) FROM controls")
    if cur.fetchone()[0] == 0:
        try:
            controls = ipeds_dict_df[ipeds_dict_df['varname'] == 'CONTROL']
            cur.executemany("""
                            INSERT INTO controls (control_id, control)
                            VALUES (%s, %s)
                            """,
                            [(row['codevalue'],  None) if row['codevalue'] == "-3" else
                                (row['codevalue'], row['valuelabel']) for i, row in controls.iterrows()])
        except psycopg.errors.UniqueViolation as e:
            print("Attempted to insert a duplicate key value:", e)
            conn.rollback()
    else:
        print("Data already loaded in controls table.")

    # regions
    cur.execute("SELECT COUNT(*) FROM regions")
    if cur.fetchone()[0] == 0:
        try:
            regions = ipeds_dict_df[ipeds_dict_df['varname'] == 'OBERREG']
            cur.executemany("""
                            INSERT INTO regions (region_id, region)
                            VALUES (%s, %s)
                            """,
                            [(row['codevalue'], row['valuelabel']) for i, row in regions.iterrows()])
        except psycopg.errors.UniqueViolation as e:
            print("Attempted to insert a duplicate key value:", e)
            conn.rollback()
    else:
        print("Data already loaded in regions table.")

    # ccs_basic
    cur.execute("SELECT COUNT(*) FROM ccs_basic")
    if cur.fetchone()[0] == 0:
        try:
            ccs_basic = ipeds_dict_df[ipeds_dict_df['varname'] == 'C21BASIC']
            cur.executemany("""
                            INSERT INTO ccs_basic (cc_basic_id, cc_basic)
                            VALUES (%s, %s)
                            """,
                            [(row['codevalue'],  None) if row['codevalue'] == "-2" else
                                (row['codevalue'], row['valuelabel']) for i, row in ccs_basic.iterrows()])
        except psycopg.errors.UniqueViolation as e:
            print("Attempted to insert a duplicate key value:", e)
            conn.rollback()
    else:
        print("Data already loaded in ccs_basic table.")

    # institutions_static
    # cur.execute("SELECT COUNT(*) FROM institutions_static")
    # if cur.fetchone()[0] == 0:
    #     try:
    #         cur.executemany("""
    #                         INSERT INTO institutions_static (
    #                         unit_id, instnm, addr, city_id, zip,
    #                         county_cd, cbsa_id, region_id, latitude, longitude, control_id,
    #                         c21basic_id, c21ipug_id, c21ipgrd_id,
    #                         c21ugprf_id, c21enprf_id, c21szset_id)
    #                         VALUES (%s, %s, %s, %s, %s,
    #                         %s, %s, %s, %s, %s, %s,
    #                         %s, %s, %s,
    #                         %s, %s, %s)
    #                     """,
    #                     [(row['UNITID'], row['INSTNM'], row['ADDR'], ) for i, row in ipeds_df.iterrows()])
    #     except Exception as e:
    #         print()

    # try:
    #     cities = ipeds_df[['CITY', 'STABBR']].drop_duplicates()
    #     cur.executemany("""
    #                     INSERT INTO cities (city_id, city, stabbr)
    #                     VALUES (%s, %s, %s)
    #                     """,
    #                     [(i + 1, row['CITY'], row['STABBR']) for i, row in cities.iterrows()])
    # except Exception as e:
    #     # if an exception/error happens in this block, Postgres goes back
    #     # to
    #     # the last savepoint upon exiting the `with` block
    #     print("insert failed:" + str(e))
    #     # add additional logging, error handling here
    #     conn.rollback()

    # else:
    #     # no exception happened, so we continue without reverting the
    #     # savepoint
    #     num_rows_inserted += len(ipeds_df)

# now we commit the entire transaction
conn.commit()
print("Number of rows inserted into tables", num_rows_inserted)

# try:
#     cur.execute("""
#                 INSERT INTO institutions_static (unit_id,
#                     agency_id,
#                     preddeg_id,
#                     highdeg_id,
#                     control_id,
#                     region_id INT REFERENCES regions (region_id),
#                     instnm TEXT,
#                     addr TEXT,
#                     city_id INT REFERENCES cities (city_id),
#                     zip INT check (length(TEXT(zip)) = 5),
#                     c21basic_id INT REFERENCES ccs_basic (cc_basic_id),
#                     c21ipug_id INT REFERENCES ccs_ipug (cc_ipug_id),
#                     c21ipgrd_id INT REFERENCES ccs_ipgrd (cc_ipgrd_id),
#                     c21ugprf_id INT REFERENCES ccs_ugprf (cc_ugprf_id),
#                     c21enprf_id INT REFERENCES ccs_enprf (cc_enprf_id),
#                     c21szset_id INT REFERENCES ccs_szset (cc_szset_id),
#                     cbsa_id INT REFERENCES cbsas (cbsa_id),
#                     county_cd INT,
#                     latitude FLOAT,
#                     longitude)
#                 VALUES (1001, '2014-11-04 09:57:50.516228', 100, 29385, 722,
#                     2, 485.200012, 'B', '')
#                 """)
# except psycopg.errors.ForeignKeyViolation as e:
#     print("Foreign key constraint violation:", e)

cur.close()
conn.close()
