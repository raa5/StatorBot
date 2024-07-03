import streamlit as st
import snowflake.connector
import pandas as pd

# Streamlit app title
st.title('Snowflake Data Viewer')

# Function to connect to Snowflake and run the query
def get_data():
    conn = snowflake.connector.connect(
       user='rohenagarwal@rivian.com',
       authenticator='externalbrowser',
       account='rivian',
       warehouse='REPORTING',
       role='REPORTER',
       database='MFG',
       schema='MES'
    )

    query = """
    SELECT DISTINCT product_serial, parameter_name, parameter_value_num, WI_SHORT_DESCRIPTION, recorded_at, result_status, overall_process_status
    FROM mfg.mes.fct_parameter_records
    WHERE line_name = 'STTR01'
      AND station_name = '065'
      AND overall_process_status = 'NOK'
      AND recorded_at > '2024-06-10 07:00:00.000'
      AND (
        (parameter_name = 'Value Height Pin X' AND (parameter_value_num < 39 OR parameter_value_num > 44.7)) OR
        (parameter_name = 'Value Pixle Area Pin X' AND (parameter_value_num < 5000 OR parameter_value_num > 12000)) OR
        (parameter_name = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_num < 2.6 OR parameter_value_num > 3.8)) OR
        (parameter_name = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_num < 1.2 OR parameter_value_num > 3.0)) OR
        (parameter_name = 'Value Angle 1 Pin X' AND (parameter_value_num < 13 OR parameter_value_num > 45)) OR
        (parameter_name = 'Value Angle 2 Pin X' AND (parameter_value_num < -45 OR parameter_value_num > -13)) OR
        (parameter_name = 'Value Level Difference' AND (parameter_value_num < 0 OR parameter_value_num > 0.6)) OR
        (parameter_name = 'Value Pin 1 edge to stack edge' AND (parameter_value_num < 33.580 OR parameter_value_num > 42.300)) OR
        (parameter_name = 'Value Pin 5 edge to stack edge' AND (parameter_value_num < 4.5 OR parameter_value_num > 12.50))
      )
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Button to load data
if st.button('Load Data'):
    with st.spinner('Loading data from Snowflake...'):
        df = get_data()
        st.success('Data loaded successfully!')
        st.write(df)

# Sidebar to adjust parameters
st.sidebar.header('Query Parameters')
line_name = st.sidebar.text_input('Line Name', 'STTR01')
station_name = st.sidebar.text_input('Station Name', '065')
overall_process_status = st.sidebar.selectbox('Overall Process Status', ['NOK', 'OK'])
recorded_at = st.sidebar.date_input('Recorded After', pd.to_datetime('2024-06-10 07:00:00'))

# Update query based on sidebar inputs
if st.sidebar.button('Update Query'):
    with st.spinner('Updating query...'):
        query = f"""
        SELECT DISTINCT product_serial, parameter_name, parameter_value_num, WI_SHORT_DESCRIPTION, recorded_at, result_status, overall_process_status
        FROM mfg.mes.fct_parameter_records
        WHERE line_name = '{line_name}'
          AND station_name = '{station_name}'
          AND overall_process_status = '{overall_process_status}'
          AND recorded_at > '{recorded_at}'
          AND (
            (parameter_name = 'Value Height Pin X' AND (parameter_value_num < 39 OR parameter_value_num > 44.7)) OR
            (parameter_name = 'Value Pixle Area Pin X' AND (parameter_value_num < 5000 OR parameter_value_num > 12000)) OR
            (parameter_name = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_num < 2.6 OR parameter_value_num > 3.8)) OR
            (parameter_name = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_num < 1.2 OR parameter_value_num > 3.0)) OR
            (parameter_name = 'Value Angle 1 Pin X' AND (parameter_value_num < 13 OR parameter_value_num > 45)) OR
            (parameter_name = 'Value Angle 2 Pin X' AND (parameter_value_num < -45 OR parameter_value_num > -13)) OR
            (parameter_name = 'Value Level Difference' AND (parameter_value_num < 0 OR parameter_value_num > 0.6)) OR
            (parameter_name = 'Value Pin 1 edge to stack edge' AND (parameter_value_num < 33.580 OR parameter_value_num > 42.300)) OR
            (parameter_name = 'Value Pin 5 edge to stack edge' AND (parameter_value_num < 4.5 OR parameter_value_num > 12.50))
          )
        """
        df = get_data()
        st.success('Query updated successfully!')
        st.write(df)
