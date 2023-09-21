# Copyright (c) 2022. MindGraph Technologies. All rights reserved.
# Proprietary and confidential. Copying and distribution is strictly prohibited.

__author__ = "Anil Turaga"
__copyright__ = "MindGraph"
__version__ = "0.1.0"
__maintainer__ = "Anil Turaga"
__email__ = "anil.turaga@mind-graph.com"
__status__ = "Development"
__date__ = "18/Jan/2023"

import streamlit as st
import pandas as pd
# initiate pyspark
import tempfile
import os
import shutil
import textdistance
# wide mode
st.set_page_config(layout="wide")
# spark = SparkSession.builder.appName('customer360.ai').getOrCreate()

# # initiate spark context
# sc = spark.sparkContext

"# CeBu Dedupe Validation App"
"> Customer360.ai"
tempdir = tempfile.mkdtemp()


def analytics(df):
    # df_new_dedupe = df_new_dedupe.withColumn("passenger_index",F.arrays_zip("passenger_hash","ProvisionalPrimaryKey","FirstName","LastName","DateOfBirth","EmailAddress","Phone","Gender")).withColumn("passenger_index", F.explode("passenger_index")).select("group_hash","passenger_index.*")
    # df_new_dedupe = df_new_dedupe.toPandas()
    # display KPIS
    df["PassengerID"] = df["PassengerID"].astype(str)
    df["PersonID"] = df["PersonID"].astype(str)
    with st.form("primary_filter"):
        "## Exploratory search"
        col1, col2, col3,col4, col5,col6, col7 = st.columns(7)
        with col1:
            FirstName = st.text_input('Enter FirstName')
        with col2:
            LastName = st.text_input('Enter LastName')
        with col3:
            DateOfBirth = st.text_input('Enter DateOfBirth')
        with col4:
            EmailAddress = st.text_input('Enter EmailAddress')
        with col5:
            Phone = st.text_input('Enter Phone')
        with col6:
            PassengerID = st.text_input('Enter PassengerID')
        with col7:
            PersonID = st.text_input('Enter PersonID')
        submitted = st.form_submit_button("Submit")
    
    # if submitted:
    df_filter = df
    df_filter['DateOfBirth'] = df_filter['DateOfBirth'].astype(str)
    if FirstName:
        df_filter = df_filter[df_filter['FirstName'].isin(FirstName.split(','))]
    if LastName:
        df_filter = df_filter[df_filter['LastName'].isin(LastName.split(','))]
    if DateOfBirth:
        df_filter = df_filter[df_filter['DateOfBirth'].isin(DateOfBirth.split(','))]
    if EmailAddress:
        df_filter = df_filter[df_filter['EmailAddress'].isin(EmailAddress.split(','))]
    if Phone:
        df_filter = df_filter[df_filter['Phone'].isin(Phone.split(','))]
    if PassengerID:
        df_filter = df_filter[df_filter['PassengerID'].isin(PassengerID.split(','))]
    if PersonID:
        df_filter = df_filter[df_filter['PersonID'].isin(PersonID.split(','))]

    st.metric("Total Records", df_filter.shape[0])
    df_new_dedupe = df_filter

    # else:
    #     df_new_dedupe = df



    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", df_new_dedupe.shape[0])
    with col2:
        st.metric("Total Groups", df_new_dedupe['group_hash'].nunique())
    with col3:
        st.metric("Total Passengers", df_new_dedupe['passenger_hash'].nunique())
    group_hash = st.selectbox('Select group_hash sorted by desc count', df_new_dedupe['group_hash'].value_counts().index)
    col1, col2, col3,col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Records", df_new_dedupe[df_new_dedupe['group_hash'] == group_hash].shape[0]) 

    with col2:
        st.metric("Non Null DOB", df_new_dedupe[(df_new_dedupe['DateOfBirth'].notnull()) & (df_new_dedupe['DateOfBirth'] != 'ccai_null')].shape[0])
    with col3:
        st.metric("Non Null Phone", df_new_dedupe[(df_new_dedupe['Phone'].notnull()) & (df_new_dedupe['Phone'] != 'ccai_null')].shape[0])
    with col4:
        st.metric("Non Null Email", df_new_dedupe[(df_new_dedupe['EmailAddress'].notnull()) & (df_new_dedupe['EmailAddress'] != 'ccai_null')].shape[0])
    with col5:
        st.metric("Non Null/FNU FirstName", df_new_dedupe[(df_new_dedupe['FirstName'].notnull()) & (df_new_dedupe['FirstName'] != 'ccai_null')& (df_new_dedupe['FirstName'] != 'FNU')].shape[0])

    df_new_dedupe[df_new_dedupe['group_hash'] == group_hash]
    df_new_dedupe_pass = df_new_dedupe[df_new_dedupe['group_hash'] == group_hash]
    passenger_hash = st.selectbox('Select passenger_hash', df_new_dedupe_pass['passenger_hash'].value_counts().index)
    st.metric("Total Records", df_new_dedupe_pass[df_new_dedupe_pass['passenger_hash'] == passenger_hash].shape[0])
    df_new_dedupe_pass[df_new_dedupe_pass['passenger_hash'] == passenger_hash]






textdistance.jaro_winkler.similarity("MariaCieloCandice", "MariaCielo")
with st.form("jaro"):
    "#### Jaro Winkler playground"
    col1, col2 = st.columns(2)
    with col1:
        name1 = st.text_input('Enter Name 1')
    with col2:
        name2 = st.text_input('Enter Name 2')
    submitted = st.form_submit_button("Compare")
if submitted:
    st.write("Similarity score:",textdistance.jaro_winkler.similarity(name1,name2))


"### Filters applied"
"Filter is applied on all combinations of firstname_filters and lastname_filters"
st.code(
"""firstname_filters = ["tbaa","pax", "fname", "xxx", "firstname","api","anonymous","xxx","cebitcc", "ntba","itcc", "sherwin"]
lastname_filters = ["tbaa", "pax", "tba", "lname", "xxx", "andreda", "commandcenter","ceb", "lastname"]
""")
uploaded_file = st.file_uploader("Upload passenger index part files", type="parquet", accept_multiple_files=True)
df_new_dedupe = False
for index,each_file in enumerate(uploaded_file):
    temp_file = os.path.join(tempdir, f"part-{index}.parquet")
    with open(temp_file, "wb") as f:
        f.write(each_file.getbuffer())
    if index == 0:
        # df_new_dedupe = spark.read.parquet(temp_file)
        df_new_dedupe = pd.read_parquet(temp_file)
    else:
        df_new_dedupe = pd.concat([df_new_dedupe, pd.read_parquet(temp_file)])
if type(df_new_dedupe) != bool:
    analytics(df_new_dedupe)

# remove the temporary directory
shutil.rmtree(tempdir)