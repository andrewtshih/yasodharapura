import sys
import pandas as pd
import psycopg
import credentials_proj

filename = sys.argv[1]

# load csv as pandas df
ipeds_df = pd.read_csv(filename, encoding='unicode_escape')
ipeds_df['ZIP'] = ipeds_df['ZIP'].str.slice(0, 5)

# preprocessing

# conversions
cities = ipeds_df[['CITY', 'STABBR']].drop_duplicates()


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
    # for i, row in cities.iterrows():
    try:
        #         # make a new SAVEPOINT -- like a save in a video game
        #         with conn.transaction():
        # perhaps a bunch of reformatting and data manipulation goes here

        # now insert the data
        # cur.execute("""
        #             INSERT INTO institutions_static (unit_id,
        #                 agency_id,
        #                 preddeg_id,
        #                 highdeg_id,
        #                 control_id,
        #                 region_id,
        #                 instnm,
        #                 addr,
        #                 city_id,
        #                 zip,
        #                 c21basic_id,
        #                 c21ipug_id,
        #                 c21ipgrd_id,
        #                 c21ugprf_id,
        #                 c21enprf_id,
        #                 c21szset_id,
        #                 cbsa_id,
        #                 county_cd,
        #                 latitude,
        #                 longitude)
        #             VALUES
        #             """,
        #             (row[]))
        cur.executemany("""
                    INSERT INTO cities (city_id, city, stabbr)
                    VALUES (%s, %s, %s)
                    """,
                        [(i + 1, row['CITY'], row['STABBR']) for i, row in cities.iterrows()])
    except Exception as e:
        # if an exception/error happens in this block, Postgres goes back
        # to
        # the last savepoint upon exiting the `with` block
        print("insert failed:" + str(e))
        # add additional logging, error handling here
    else:
        # no exception happened, so we continue without reverting the
        # savepoint
        num_rows_inserted += 1

# now we commit the entire transaction
conn.commit()

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
