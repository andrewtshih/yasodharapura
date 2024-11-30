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


states = conn.query("""SELECT UNIQUE STABBR
                    FROM cities
                    ORDER BY STABBR DESC""")
states.append("All")
line_state = st.selectbox(
    "State",
    states
)
if line_state == "All":
    line_state_fact = ""
else:
    line_state_fact = ", STABBR == " + line_state


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
