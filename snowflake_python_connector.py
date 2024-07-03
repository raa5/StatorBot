# # # import snowflake.connector

# # # # Get your credentials

# # # conn = snowflake.connector.connect(
# # # user="ROHENAGARWAL@RIVIAN.COM",
# # # password="ArmySpy#1007",
# # # account="rivian",
# # # warehouse='REPORTING',
# # # database='MFG',
# # # schema='MES',
# # # insecure_mode=True  # This bypasses SSL verification
# # # )

# # # # Create a cursor object

# # # cur = conn.cursor()

# # # # Define your query

# # # query = """
# # # SELECT DISTINCT product_serial, parameter_name, MAX(parameter_value_num), recorded_at
# # # FROM mfg.mes.fct_parameter_records
# # # WHERE line_name = 'STTR01'
# # # AND station_name = '130'
# # # AND recorded_at > '2024-06-21'
# # # AND parameter_name ILIKE 'Weight current%'
# # # -- AND product_serial IN ('24183ADU2ST1167579')
# # # -- '24165ADU2ST1158554',
# # # -- '24166ADU2ST1159035')
# # # -- AND parameter_value_num > 0
# # # GROUP BY product_serial, parameter_name, recorded_at
# # # ORDER BY product_serial, recorded_at DESC
# # # """

# # # # Execute the query

# # # cur.execute(query)

# # # # Fetch results

# # # results = cur.fetchall()

# # # # Print results

# # # for row in results:
# # #     print(row)

# # # # Close the cursor

# # # cur.close()


# # # # import snowflake.connector
# # # # import logging

# # # # # Enable logging
# # # # for logger_name in ['snowflake.connector', 'botocore']:
# # # #     logger = logging.getLogger(logger_name)
# # # #     logger.setLevel(logging.DEBUG)
# # # #     ch = logging.StreamHandler()
# # # #     ch.setLevel(logging.DEBUG)
# # # #     logger.addHandler(ch)

# # # # # Simplified connection script with logging
# # # # try:
# # # #     conn = snowflake.connector.connect(
# # # #         user='ROHENAGARWAL@RIVIAN.COM',
# # # #         password='ArmySpy#1007',
# # # #         account='rivian',
# # # #         warehouse='REPORTING',
# # # #         database='MFG',
# # # #         schema='MES',
# # # #         insecure_mode=True  # This bypasses SSL verification
# # # #     )
# # # #     print("Connection successful!")
# # # #     conn.close()
# # # # except snowflake.connector.errors.OperationalError as e:
# # # #     print(f"OperationalError: {e}")



# # import os
# # import snowflake.connector
# # from azure.identity import DefaultAzureCredential

# # # Set Snowflake connection parameters
# # snowflake_account = 'rivian'
# # snowflake_user = 'rohenagarwal'
# # snowflake_database = 'MFG'
# # snowflake_schema = 'MES'
# # snowflake_warehouse = 'REPORTING'

# # # Initialize Azure credentials
# # credential = DefaultAzureCredential()

# # try:
# #     # Obtain an access token from Azure AD
# #     token = credential.get_token('https://snowflake.azureexternal.com/.default')

# #     # Connect to Snowflake using the access token
# #     conn = snowflake.connector.connect(
# #         user=snowflake_user,
# #         account=snowflake_account,
# #         authenticator='oauth',
# #         token=token.token,
# #         warehouse=snowflake_warehouse,
# #         database=snowflake_database,
# #         schema=snowflake_schema
# #     )

# #     # Create a cursor object
# #     cur = conn.cursor()

# #     # Define your query
# #     query = """
# #     SELECT DISTINCT product_serial, parameter_name, MAX(parameter_value_num), recorded_at
# #     FROM mfg.mes.fct_parameter_records
# #     WHERE line_name = 'STTR01'
# #     AND station_name = '130'
# #     AND recorded_at > '2024-06-21'
# #     AND parameter_name ILIKE 'Weight current%'
# #     GROUP BY product_serial, parameter_name, recorded_at
# #     ORDER BY product_serial, recorded_at DESC
# #     """

# #     # Execute the query
# #     cur.execute(query)

# #     # Fetch results
# #     results = cur.fetchall()

# #     # Print results
# #     for row in results:
# #         print(row)

# #     # Close the cursor
# #     cur.close()
# #     conn.close()

# # except Exception as e:
# #     print(f"Error: {e}")


# #!/usr/bin/env python
# import snowflake.connector

# # Gets the version
# ctx = snowflake.connector.connect(
#     user='rohenagarwal@rivian.com',
#     authenticator='externalbrowser',
#     account='rivian'
#     )
# cs = ctx.cursor()
# try:
#     cs.execute("SELECT current_version()")
#     one_row = cs.fetchone()
#     print(one_row[0])
# finally:
#     cs.close()
# ctx.close()


import snowflake.connector
import pandas as pd

conn = snowflake.connector.connect(
   user='rohenagarwal@rivian.com',
   authenticator='externalbrowser',
   account='rivian',
   warehouse='REPORTING',
   role='REPORTER',
   database='MFG',
   schema='MES'
)

query = """SELECT DISTINCT product_serial, parameter_name, parameter_value_num, WI_SHORT_DESCRIPTION, recorded_at, result_status, overall_process_status
-- SELECT DISTINCT product_serial, parameter_name, parameter_value_num, recorded_at, result_status, overall_process_status
FROM mfg.mes.fct_parameter_records
WHERE line_name = 'STTR01'
  AND station_name = '065'
  AND overall_process_status = 'NOK'
  AND recorded_at > '2024-06-10 07:00:00.000'
  -- AND recorded_at < '2024-06-12 07:00:00.000'
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
  -- and parameter_value_num != 0
  -- and product_serial = '24169ADU2ST1159978'
"""
df = pd.read_sql(query, conn)
print(df)
