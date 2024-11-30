import streamlit as st


st.title("College statistics dashboard")


@st.cache_resource
def get_conn(database):
    return st.connection(database)
conn = get_conn("your_connection")


"Institutions by loan repayment rates"


loan_qual = st.selectbox(
    "Sort by:",
    ("Highest", "Lowest")
)
loans_dict = {"Highest" : " DESC", "Lowest" : ""}


loan_num = st.slider("Number to show", 1, 20, 10)


loan_df = conn.query("""SELECT instnm AS Institution,
                     cdr2 AS Repayment
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id = institutions_non_static.unit_id
                     ORDER BY cdr2""" + loans_dict[loan_qual] + """
                     LIMIT """ + str(loan_num))
loan_df


"Line graph of chosen statistic over time"


line_inst = st.selectbox(
    "Institution type",
    ("Public", "Private not-for-profit", "Private for-profit", "All")
)
if line_inst == "All":
    line_inst_fact = "1, 2, 3"
else:
    inst_dict = {"Public" : "1",
                 "Private not-for-profit" : "2",
                 "Private for-profit" : "3"}
    line_inst_fact = "WHERE control_id == " + inst_dict[line_inst]


states = conn.query("""SELECT UNIQUE stabbr
                    FROM cities
                    ORDER BY stabbr DESC""")
states.append("All")
line_state = st.selectbox(
    "State",
    states
)
if line_state == "All":
    line_state_fact = ""
else:
    line_state_fact = ", stabbr == " + line_state


line_factor = st.selectbox(
    "Factor",
    ("Tuition rate", "Loan repayment rate", "Admission rate")
)
factor_dict = {"Tuition rate" : "tuitionfee_prog",
               "Loan repayment rate" : "cdr2",
               "Admission rate" : "adm_rate"}


line_aggr = st.selectbox(
    "Aggregation type",
    ("Count", "Average")
)
aggr_dict = {"Count" : "COUNT(",
             "Average" : "AVG("}


line_df = conn.query("""SELECT year AS Year,
                     """ + aggr_dict[line_aggr] + factor_dict[line_factor] + ") AS " + line_factor + """
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id = institutions_non_static.unit_id
                     INNER JOIN cities ON
                     institutions_static.city_id = cities.city_id
                     WHERE control_id IN (""" + line_inst_fact + ")" + line_state_fact + """
                     GROUP BY Year""")


st.line_chart(line_df)


"Summary statistics"


aggr_year = st.selectbox(
    "Year",
    ("2019", "2020", "2021", "2022")
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
    aggr_state_fact3 = ", stabbr DESC"


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


aggr_cc = st.selectbox(
    "Carnegie classification",
    ("No", "Yes")
)
if aggr_cc == "No":
    aggr_cc_fact1 = ""
    aggr_cc_fact2 = ""
    aggr_cc_fact3 = ""
else:
    aggr_cc_fact1 = "cc_basic AS CarnegieClassification,"
    aggr_cc_fact2 = ", cc_basic"
    aggr_cc_fact3 = ", cc_basic DESC"


aggr_accr = st.selectbox(
    "Accreditation agency",
    ("No", "Yes")
)
if aggr_accr == "No":
    aggr_accr_fact1 = ""
    aggr_accr_fact2 = ""
    aggr_accr_fact3 = ""
else:
    aggr_accr_fact1 = "accred_agency AS AccreditationAgency,"
    aggr_accr_fact2 = ", accred_agency"
    aggr_accr_fact3 = ", accred_agency DESC"


aggr_df = conn.query("""SELECT year as Year,
                     """ + aggr_state_fact1 + aggr_inst_fact1 + aggr_cc_fact1 + aggr_accr_fact1 """
                     COUNT(year) AS Count,
                     AVG(tuitfte) AS AvgTuition,
                     AVG(actcmmid) AS AvgACT
                     FROM institutions_static
                     INNER JOIN institutions_non_static ON
                     institutions_static.unit_id = institutions_non_static.unit_id
                     INNER JOIN cities ON
                     institutions_static.city_id = cities.city_id
                     INNER JOIN controls ON
                     institutions_static.control_id = controls.control_id
                     INNER JOIN ccs_basic ON
                     institutions_static.cc_basic_id = ccs_basic.cc_basic_id
                     INNER JOIN accred_agencies ON
                     institutions_non_static.agency_id = accred_agencies.agency_id
                     WHERE year == """ + aggr_year + """
                     GROUP BY year
                     """ + aggr_state_fact2 + aggr_inst_fact2 + aggr_cc_fact2 + aggr_accr_fact2 """
                     ORDER BY year""" + aggr_state_fact3 + aggr_inst_fact3 + aggr_cc_fact3 + aggr_accr_fact3)
