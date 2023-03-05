import data_centers as db
from datetime import datetime, timedelta

# connect to sql server
sql_server = db.sql_server

# define formatted date parameters: yesterday = sus format, today = timestamp format
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')


# function to pull all pricing agreements keyed day prior with customer detail out of 
# sus and return a list of records.
def lead_agreements_created_yesterday():

    # establish connection to sus as240a
    sus = db.sus('240')

    # pull all pricing agreements keyed day prior with customer detail out of sus
    rows = sus.execute(f'''
        SELECT 
        CAST(M7VAGN AS VARCHAR(11)) AS VA, 
        CAST(M7ACAN AS VARCHAR(11)) AS CA, 
        AZPCIE AS IEA, 
        AZPCSC AS SPEC_CODE, 
        TRIM(AZPCSP) AS SPEC, 
        LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
        LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
        '{today}' AS TIMESTAMP

        FROM SCDBFP10.PMVHM7PF 

        INNER JOIN SCDBFP10.USCNAZL0 
        ON RIGHT('000' || M7VAGN, 9) = AZCEEN 
        AND AZCEAI = 'VA ' 

        WHERE M7EADT = {yesterday}
        AND M7ACAN = 0

        UNION

        SELECT 
        CAST(M7VAGN AS VARCHAR(11)) AS VA, 
        CAST(M7ACAN AS VARCHAR(11)) AS CA, 
        AZPCIE AS IEA, 
        AZPCSC AS SPEC_CODE, 
        TRIM(AZPCSP) AS SPEC, 
        LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
        LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
        '{today}' AS TIMESTAMP

        FROM SCDBFP10.PMVHM7PF 

        INNER JOIN SCDBFP10.USCNAZL0 
        ON RIGHT('000' || M7ACAN, 9) = AZCEEN 
        AND AZCEAI = 'CA ' 

        WHERE M7EADT = {yesterday}
        AND M7ACAN <> 0

        UNION

        SELECT '0' AS VA, 
        CAST(NHCANO AS VARCHAR(11)) AS CA, 
        ZPCIE AS IEA, 
        AZPCSC AS SPEC_CODE,
        TRIM(AZPCSP) AS SPEC, 
        LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
        LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
        '{today}' AS TIMESTAMP

        FROM SCDBFP10.PMPVNHPF

        INNER JOIN SCDBFP10.USCNAZL0 
        ON RIGHT('000' || NHCANO, 9) = AZCEEN 
        AND AZCEAI = 'CA ' 

        WHERE NHEADT = {yesterday}
        AND NHCVAN = 0
    ''').fetchall()

    # return list of values
    return rows


# insert dataset into sql server. dataset must have same formatting and data fields as server table
def insert_into_sql_server(dataset, table):

    # get row count of dataset
    rowCount = len(dataset)

    # sql server has an insert row max of 1000 records - handle differently depending on size of dataset
    if rowCount > 1000:

        # loop through dataset in chuncks of 1000 records
        for i in range(0, rowCount, 1000):

            # format chunk of 1000 insert into sql server
            rows = ','.join(str(row) for row in dataset[i:i+1000])
            sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
            
        # commit insert statement after all chunks have been loaded in the server
        sql_server.commit()
    else:

        # format dataset, insert into sql server and commit
        rows = ','.join(str(row) for row in dataset)
        sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
        sql_server.commit()


# function to pull all customer specs with account ties in alaska and return a list of records.
def alaska_account_ties(specs):

    # establish connection to sus as450a
    sus = db.sus('450')

    # pull all customer specs with account ties in alaska
    rows = sus.execute(f'''
        SELECT DISTINCT TRIM(AZCEEN) AS SPEC

        FROM SCDBFP10.USCNAZL0 

        WHERE TRIM(AZCEEN) IN ({specs}) 
        AND AZEFED >= {today}
        AND AZCEAI = 'GRP' 
    
        UNION 

        SELECT DISTINCT 
        IFNULL(TRIM(T1.JTTPAR), TRIM(T2.JTTPAR)) AS SPEC 
        FROM SCDBFP10.USCBJOPF 
        LEFT JOIN SCDBFP10.USCKJTPF AS T1 
        ON JOCUNO = T1.JTFPAR 
        AND T1.JTFTYP NOT IN ('PRNT', 'MSTR') 
        AND T1.JTTTYP IN ('PRNT', 'MSTR') 
        AND T1.JTTEDT >= {today} 
        LEFT JOIN SCDBFP10.USCKJTPF AS T2 
        ON TRIM(JOPICU) = TRIM(T2.JTFPAR) 
        AND T2.JTFTYP NOT IN ('PRNT', 'MSTR')
        AND T2.JTTTYP IN ('PRNT', 'MSTR') 
        AND T2.JTTEDT >= {today} 
        WHERE TRIM(T1.JTTPAR) IN ({specs}) 
        OR TRIM(T2.JTTPAR) IN ({specs})
    ''').fetchall()

    return rows


# function to delete all lead agreements from the alaska_cutomer_eligibility table where there 
# are no active customer ties in alaska. 
def delete_irrelevent_agreements(dataset):

    # format string of customer specs which do have ties in alaska
    rows = "'" + "','".join(str(row.SPEC) for row in dataset) + "'"

    # delete all lead agreements where there are no customer specs with account ties in alaska 
    # and commit transaction
    sql_server.execute(f'''
        DELETE 
        
        FROM Alaska_Customer_Eligibility
        
        WHERE VA NOT IN (
            SELECT VA 

            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({rows})
            AND VA <> '0'
        )
        AND CA NOT IN (
            SELECT CA 
            
            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({rows})
            AND CA <> '0'
        )
        AND TIMESTAMP = '{today}'
    ''')
    sql_server.commit()


# function to pull all lead agreements created <yesterday>, and store the agreements that have 
# customer specs with account ties in alaska in sql serer table alaska_customer_eligibility.
def upload_alaska_deviations():

    # get list of all agreements created yesterday with customer detail
    allAgreements = lead_agreements_created_yesterday(sus)

    # if there are no agreements loaded <yesterday>, end function
    if len(allAgreements) > 0:

        # insert agreement/customer eligibility dataset into sql server
        insert_into_sql_server(allAgreements, 'Alaska_Customer_Eligibility')

        # format sql string of all customer specs on agreements loaded <yesterday>
        allSpecs = "'" + "','".join(str(row.SPEC) for row in allAgreements) + "'"

        # get list of all customer specs with account ties in alska
        alaskaSpecs = alaska_account_ties(allSpecs)

        # delete agreements from sql server that do not have specs attached with account ties in alaska
        delete_irrelevent_agreements(alaskaSpecs)



