import streamlit as st
import pandas as pd
import credentials_proj
import plotly.express as px
import matplotlib.pyplot as plt
from loading_helper_functions import conn_cur


st.title("College Scorecard Dashboard")


# @st.cache_resource
# def get_conn(database):
#     return st.connection(database)

# conn = st.connection("sql")

# conn = psycopg.connect(
#     host="pinniped.postgres.database.azure.com",
#     dbname=credentials_proj.dbname,
#     user=credentials_proj.DB_USER,
#     password=credentials_proj.DB_PASSWORD
# )

host = "pinniped.postgres.database.azure.com"
dbname = credentials_proj.dbname
credentials_module = credentials_proj

conn, cur = conn_cur(host, dbname, credentials_module)


st.header("Institutions ranked by loan repayment rates")
"""Ranks top or bottom institutions by 3-year default rate (lower number
= better repayment). Number of institutions shown is chosen by slider.
Ties between institutions are broken alphabetically."""


loan_qual = st.selectbox(
    "Sort by:",
    ("Highest", "Lowest")
)
loans_dict = {"Highest": " DESC", "Lowest": ""}

year_select = st.selectbox(
    "Year",
    ("2018-19", "2019-20", "2020-21", "2021-22")
)


loan_num = st.slider("Number to show", 1, 20, 10)

cur.execute("""SELECT instnm AS Institution,
                     cdr3 AS Repayment
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id =
                     institutions_non_static.unit_id
                     WHERE cdr3 IS NOT NULL
                     AND year = '""" + year_select + """'
                     ORDER BY cdr3""" + loans_dict[loan_qual] + """, instnm
                     LIMIT """ + str(loan_num))

results = cur.fetchall()

loan_df = pd.DataFrame(results, columns=["Institution",
                        """Three-year cohort default loan repayment rate"""])

st.dataframe(loan_df, hide_index=True)


st.header("Chosen rate over time")
"""Shows average rate from three available rates over every year of the data.
Possible rates are tuition rate, loan repayment rate, and admission rate. Can
narrow down by state or institution type. The tuition rate is represented by the
average tuition and fees for program-year institutions."""


line_inst = st.selectbox(
    "Institution type",
    ("All", "Public", "Private not-for-profit", "Private for-profit")
)
if line_inst == "All":
    line_inst_fact = "1, 2, 3"
else:
    inst_dict = {"Public": "1",
                 "Private not-for-profit": "2",
                 "Private for-profit": "3"}
    line_inst_fact = inst_dict[line_inst]


cur.execute("""SELECT DISTINCT stabbr
                    FROM cities
                    ORDER BY stabbr""")
results = [i[0] for i in cur.fetchall()]
results = ["All"] + results
states = pd.DataFrame(results, columns=["State"]).reset_index(drop=True)
# states.loc[50] = "All"
line_state = st.selectbox(
    "State",
    states
)
if line_state == "All":
    line_state_fact = ""
else:
    line_state_fact = " AND stabbr = '" + line_state + "'"


line_factor = st.selectbox(
    "Factor",
    ("Tuition rate", "Loan repayment rate", "Admission rate")
)
factor_dict = {"Tuition rate": "tuitionfee_prog",
               "Loan repayment rate": "cdr3",
               "Admission rate": "adm_rate"}


cur.execute("""SELECT year AS Year,
                     AVG(""" + factor_dict[line_factor] +
                     ") AS " + factor_dict[line_factor] + """
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id =
                     institutions_non_static.unit_id
                     INNER JOIN cities ON
                     institutions_static.city_id = cities.city_id
                     WHERE control_id IN (""" + line_inst_fact + ")" +
                     line_state_fact + """
                     AND """ + factor_dict[line_factor] + """ IS NOT NULL
                     GROUP BY Year""")


results = cur.fetchall()

line_df = pd.DataFrame(results, columns=["Year", line_factor])
line_df[line_factor] = pd.to_numeric(line_df[line_factor])


st.line_chart(data=line_df,
              x="Year",
              y=line_factor)


st.header("Count Summary of Institutions")
"""Shows aggregate number of institutions, average tuition, and average median
ACT score for a chosen year in the data. Can partition by any combination of
state, and/or institution type. Carnegie classification is an optional filter
available when the selected year is 2021-22 as the particular classification
used for this analysis was given in 2021. When a value is None, this means that
no values are available for that particular variable."""


aggr_year = st.selectbox(
    "Select Year",
    ("2018-19", "2019-20", "2020-21", "2021-22")
)

aggr_state = st.selectbox(
    "State",
    ("No", "Yes")
)
if aggr_state == "No":
    aggr_state_fact1 = ""
    aggr_state_fact2 = ""
    aggr_state_fact3 = ""
else:
    aggr_state_fact1 = "stabbr AS State,"
    aggr_state_fact2 = ", stabbr"
    aggr_state_fact3 = ", stabbr"


aggr_inst = st.selectbox(
    "Institution type",
    ("No", "Yes")
)
if aggr_inst == "No":
    aggr_inst_fact1 = ""
    aggr_inst_fact2 = ""
    aggr_inst_fact3 = ""
else:
    aggr_inst_fact1 = "control AS InstitutionType,"
    aggr_inst_fact2 = ", control"
    aggr_inst_fact3 = ", control DESC"

if aggr_year == "2021-22":
    aggr_cc = st.selectbox(
        "Carnegie classification",
        ("No", "Yes")
    )
    if aggr_cc == "No":
        aggr_cc_fact1 = ""
        aggr_cc_fact2 = ""
        aggr_cc_fact3 = ""
        ccs_basic_join = ""
    else:
        aggr_cc_fact1 = "cc_basic AS CarnegieClassification,"
        aggr_cc_fact2 = ", cc_basic"
        aggr_cc_fact3 = ", cc_basic"
        ccs_basic_join = """INNER JOIN ccs_basic ON 
            institutions_static.c21basic_id = ccs_basic.cc_basic_id"""
else:
    aggr_cc = "No"
    aggr_cc_fact1 = ""
    aggr_cc_fact2 = ""
    aggr_cc_fact3 = ""
    ccs_basic_join = ""

# aggr_cc = st.selectbox(
#     "Carnegie classification",
#     ("No", "Yes")
# )
# if aggr_cc == "No" or aggr_year != "2021-22":
#     aggr_cc_fact1 = ""
#     aggr_cc_fact2 = ""
#     aggr_cc_fact3 = ""
# else:
#     aggr_cc_fact1 = "cc_basic AS CarnegieClassification,"
#     aggr_cc_fact2 = ", cc_basic"
#     aggr_cc_fact3 = ", cc_basic"


# aggr_accr = st.selectbox(
#     "Accreditation agency",
#     ("No", "Yes")
# )
# if aggr_accr == "No":
#     aggr_accr_fact1 = ""
#     aggr_accr_fact2 = ""
#     aggr_accr_fact3 = ""
# else:
#     aggr_accr_fact1 = "accred_agency AS AccreditationAgency,"
#     aggr_accr_fact2 = ", accred_agency"
#     aggr_accr_fact3 = ", accred_agency"


cur.execute("""SELECT year as Year,
                     """ +
                     aggr_state_fact1 + aggr_inst_fact1 + aggr_cc_fact1 +
                     """
                     COUNT(year) AS Count,
                     CAST(AVG(tuitfte) as money) AS "AvgTuition",
                     ROUND(CAST(AVG(actcmmid) as numeric), 1) AS AvgACT
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id =
                        institutions_non_static.unit_id
                     INNER JOIN cities ON
                     institutions_static.city_id = cities.city_id
                     INNER JOIN controls ON
                     institutions_static.control_id = controls.control_id
                     """ + ccs_basic_join + """
                     WHERE year = '""" + aggr_year + """'
                     GROUP BY year
                     """ + aggr_state_fact2 + aggr_inst_fact2 + aggr_cc_fact2 +
                     """
                     ORDER BY year""" + aggr_state_fact3 + aggr_inst_fact3 +
                     aggr_cc_fact3
                     )


results = cur.fetchall()

cols = ["Year"]
if aggr_state == "Yes":
    cols.append("State")
if aggr_inst == "Yes":
    cols.append("Institution Type")
if aggr_cc == "Yes":
    cols.append("Carnegie Classification")
cols.append("Count")
cols.append("Average Tuition ($)")
cols.append("Average Median ACT Score")
aggr_df = pd.DataFrame(results, columns=cols)

st.dataframe(aggr_df, hide_index=True)

st.header("Number of institutions by accreditation agency in 2021-22")
"""The bar chart below shows what agencies accredit the most institutions.
Only agencies that accredit over 75 institutions are included.
Over 16% of institutions in the U.S. are not accredited."""

cur.execute("""SELECT COUNT(i.unit_id), a.accred_agency
                FROM institutions_non_static as i
                JOIN accred_agencies as a
                ON i.agency_id = a.agency_id
                WHERE i.year = '2021-22'
                GROUP BY a.accred_agency
                ORDER BY count desc""")


results = cur.fetchall()

acc_df = pd.DataFrame(results,
                      columns=["Count",
                               "Accreditation Agency"]).fillna("No Agency")
acc_df["Accreditation Agency"] = acc_df.apply(lambda x: x["Accreditation Agency"] 
                                          if x["Count"] >= 75 else "Other",
                                          axis=1)
acc_df = (acc_df.groupby("Accreditation Agency")["Count"].sum()
          .reset_index().sort_values("Count"))

fig, ax = plt.subplots(figsize=(20, 20))
ax.pie(acc_df["Count"], labels=acc_df["Accreditation Agency"],
       autopct="%1.1f%%",
       textprops={'fontsize': 30})
ax.axis("equal")
st.pyplot(fig)

st.header("New institutions")
"The table below shows what institutions are new to the dataset that year."

new_year = st.selectbox(
    "Choose Year",
    ("2019-20", "2020-21", "2021-22")
)
new_dict = {"2019-20": "2018-19",
            "2020-21": "2019-20",
            "2021-22": "2020-21"}

cur.execute("""SELECT a.year,
            a.instnm,
            a.city,
            a.stabbr
            FROM (SELECT i.instnm,
                c.city,
                c.stabbr,
                i.unit_id,
                n.year
                FROM institutions_static AS i
                INNER JOIN institutions_non_static AS n ON
                i.unit_id = n.unit_id
                INNER JOIN cities AS c ON
                i.city_id = c.city_id
                WHERE n.year = '""" + new_year + """') AS a
            LEFT JOIN (SELECT unit_id
                FROM institutions_non_static AS n
                WHERE year = '""" + new_dict[new_year] + """') AS b
                ON a.unit_id = b.unit_id
            WHERE b.unit_id IS NULL""")

results = cur.fetchall()

new_df = pd.DataFrame(results,
                      columns=["Year",
                               "Institution Name",
                               "City",
                               "State"])

st.dataframe(new_df, hide_index=True)

cur.close()
conn.close()
