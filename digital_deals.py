import data_centers as db
from datetime import datetime, timedelta, date

# define formatted date parameters: yesterday = sus format, today = timestamp format
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')


# function to pull all pricing agreements keyed day prior with customer detail out of 
# sus and return a list of records.
def lead_agreements_created_yesterday():

    # establish connection to sus as240a
    sus = db.sus('055')

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
        '{today}' AS TIMESTAMP,
        'VA' AS ELIGIBILITY_TYPE

        FROM SCDBFP10.PMVHM7PF 

        INNER JOIN SCDBFP10.USCNAZL0 
        ON RIGHT('000' || M7VAGN, 9) = AZCEEN 
        AND AZCEAI = 'VA ' 

        WHERE M7EADT between 20230526 and {yesterday}
        AND M7ACAN = 0
        AND M7VAGD NOT LIKE '%VOID%' 
        AND M7AGRN = 999999999

        UNION

        SELECT 
        CAST(M7VAGN AS VARCHAR(11)) AS VA, 
        CAST(M7ACAN AS VARCHAR(11)) AS CA, 
        T1.AZPCIE AS IEA, 
        T1.AZPCSC AS SPEC_CODE, 
        TRIM(T1.AZPCSP) AS SPEC, 
        LEFT(RIGHT(T1.AZEFSD,4),2) || RIGHT(T1.AZEFSD,2) || RIGHT(LEFT(T1.AZEFSD,4),2) AS START_DT, 
        LEFT(RIGHT(T1.AZEFED,4),2) || RIGHT(T1.AZEFED,2) || RIGHT(LEFT(T1.AZEFED,4),2) AS END_DT,
        '{today}' AS TIMESTAMP,
        CASE WHEN T2.AZCEEN IS NULL THEN 'CA' ELSE 'VA|CA' END AS ELIGIBILITY_TYPE

        FROM SCDBFP10.PMVHM7PF 

        INNER JOIN SCDBFP10.USCNAZL0 AS T1
        ON RIGHT('000' || M7ACAN, 9) = T1.AZCEEN 
        AND T1.AZCEAI = 'CA ' 

        LEFT JOIN  SCDBFP10.USCNAZL0 AS T2
        ON RIGHT('000' || M7VAGN, 9) = T2.AZCEEN 
        AND T2.AZCEAI = 'VA ' 
        AND T1.AZPCSP = T2.AZPCSP

        WHERE M7EADT between 20230526 and {yesterday}
        AND M7ACAN <> 0
        AND M7VAGD NOT LIKE '%VOID%' 
        AND M7AGRN = 999999999
    ''').fetchall()

    # close sus connection
    sus.close()

    # return list of values
    return rows


# insert dataset into sql server. dataset must have same formatting and data fields as server table
def insert_into_sql_server(dataset, table):

    # get row count of dataset
    row_count = len(dataset)

    # sql server has an insert row max of 1000 records - handle differently depending on size of dataset
    if row_count > 1000:

        # loop through dataset in chuncks of 1000 records
        for i in range(0, row_count, 1000):

            # format chunk of 1000 insert into sql server
            rows = ','.join(str(row) for row in dataset[i:i+1000])
            db.sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
            
        # commit insert statement after all chunks have been loaded in the server
        db.sql_server.commit()
    elif row_count > 0:

        # format dataset, insert into sql server and commit
        rows = ','.join(str(row) for row in dataset)
        db.sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
        db.sql_server.commit()


# function to pull all customer specs with account ties in alaska and return a list of records.
def alaska_accounts(dataset):

    # establish connection to sus as450a
    sus = db.sus('450')

    # format sql string of all customer specs on agreements loaded <yesterday>
    specs = "'" + "','".join(str(row.SPEC) for row in dataset) + "'"

    # pull all customer specs with account ties in alaska
    active_ties = sus.execute(f'''
        SELECT DISTINCT TRIM(AZCEEN) AS SPEC

        FROM SCDBFP10.USCNAZL0 

        WHERE TRIM(AZCEEN) IN ({specs}) 
        AND AZEFED >= {today}
        AND AZCEAI = 'GRP' 
    
        UNION 

        SELECT DISTINCT TRIM(JTHIMA)

        FROM SCDBFP10.USCKJTPF

        WHERE TRIM(JTHIMA) IN ({specs})
        AND JTTTYP = 'PRNT'
        AND JTFTYP NOT IN ('PRNT','MSTR')
        AND JTTEDT >= {today}

        UNION 

        SELECT DISTINCT TRIM(JTTPAR)

        FROM SCDBFP10.USCKJTPF

        WHERE TRIM(JTTPAR) IN ({specs})
        AND JTTTYP = 'PRNT'
        AND JTFTYP NOT IN ('PRNT','MSTR')
        AND JTTEDT >= {today}
    ''').fetchall()

    # pull all customer specs that exist in alaska
    active_specs = sus.execute(f'''
        SELECT DISTINCT TRIM(JUCEEN) AS SPEC

        FROM SCDBFP10.USCLJUPF 

        WHERE TRIM(JUCEEN) IN ({specs}) 
    
        UNION 

        SELECT DISTINCT 
        TRIM(JTFPAR) AS SPEC

        FROM SCDBFP10.USCKJTPF 
        
        WHERE JTFTYP IN ('PRNT', 'MSTR') 
        AND TRIM(JTFPAR) IN ({specs}) 
    ''').fetchall()

    # compile dictionary of active ties and active specs
    customers = {
        'account_ties':active_ties,
        'active_specs':active_specs
    }

    # close sus connection
    sus.close()

    return customers


# function to delete all lead agreements from the alaska_cutomer_eligibility table where there 
# are no active customer ties in alaska. return dictionary of <relevent> vendor and customer deals. 
def delete_agreements_without_customers(dataset):

    # format string of customer specs which do have ties in alaska
    account_ties = "'" + "','".join(str(row.SPEC) for row in dataset['account_ties']) + "'"
    active_specs = "'" + "','".join(str(row.SPEC) for row in dataset['active_specs']) + "'"

    # delete all lead agreements where there are no customer specs with account ties in alaska 
    # and commit transaction
    db.sql_server.execute(f'''
        BEGIN TRANSACTION

        DELETE 
        
        FROM Alaska_Customer_Eligibility
        
        WHERE SPEC NOT IN ({active_specs})
        AND TIMESTAMP = '{today}'

        DELETE 
        
        FROM Alaska_Customer_Eligibility
        
        WHERE VA NOT IN (
            SELECT VA 

            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({account_ties})
            AND VA <> '0'
        )
        AND CA NOT IN (
            SELECT CA 
            
            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({account_ties})
            AND CA <> '0'
        )
        AND TIMESTAMP = '{today}'

        COMMIT
    ''')
    db.sql_server.commit()

    # get list of <relevent> vendor agreements (va <> 0)
    va = db.sql_server.execute(f'''
        SELECT DISTINCT VA
        
        FROM Alaska_Customer_Eligibility
        
        WHERE VA <> 0 
        AND TIMESTAMP = '{today}'
    ''').fetchall()

    # get list of <relevent> customer agreements (ca <> 0)
    ca = db.sql_server.execute(f'''
        SELECT DISTINCT CA
        
        FROM Alaska_Customer_Eligibility
        
        WHERE VA = 0
        AND TIMESTAMP = '{today}'
    ''').fetchall()

    # create dictionary containing list of vendor and customer agreements to be returned
    deviations = {
        'va':va,
        'ca':ca
    }

    # return dictionary of <relevent> vendor and customer agreements
    return deviations


# parse dictionary to format values of dictionary items with va/ca keys into sql string
def parse_agreement_dictionary(agreements):

    # parse dictionary into sql strings. if either va or ca key has no items then assign
    # 404 so the query does not error out
    for key in agreements:
        if len(agreements[key]) > 0:
            agreements[key] = ",".join(str(row[0]) for row in agreements[key]) 
        else:
            agreements[key] = '404'

    # return formatted dictoinary
    return agreements


# function to get all agreement header details from sus as240a and return list of values
def agreement_header(agreements):

    # establish connection to sus as240a
    sus = db.sus('055')

    # clean dictionary values for use in query
    agreement_dictionary = parse_agreement_dictionary(agreements)

    # set query string for vendor and customer agreements 
    va = agreement_dictionary['va']
    ca = agreement_dictionary['ca']

    # get all agreement header details from sus as240a
    rows = sus.execute(f'''
        SELECT DISTINCT
        CAST(M7VAGN AS VARCHAR(11)) AS LEAD_VA, 
        CAST(M7ACAN AS VARCHAR(11)) AS LEAD_CA, 
        M7AGTY AS VA_TYPE,
        TRIM(M7VNBR) AS VENDOR_NBR, 
        TRIM(M7VAGD) AS DESCRIPTION, 
        TRIM(M7PDDD) AS PAST_DUE_DEDUCT, 
        IFNULL(NHAGTY, '') AS CA_TYPE,  
        IFNULL(NHAGTP, '') AS REBATE_TYPE, 
        M7COSP AS COST_BASIS, 
        'F' AS ALASKA_COST_BASIS, 
        LEFT(RIGHT(M7VASD,4),2) || RIGHT(M7VASD,2) || RIGHT(LEFT(M7VASD,4),2) AS START_DT, 
        LEFT(RIGHT(M7VAED,4),2) || RIGHT(M7VAED,2) || RIGHT(LEFT(M7VAED,4),2) AS END_DT,
        M7FRQC AS BILLING_FREQ, 
        CAST(M7DYNO AS VARCHAR(5)) AS BILLING_DAY, 
        M7DOW AS BILLING_DOW, 
        M7BBKF AS BILLBACK_FORMAT, 
        TRIM(M7VPAN) AS PRE_APPROVAL,
        M7COCM AS CORP_CLAIMED, 
        TRIM(M7APNM) AS APPROP_NAME,
        '{today}' AS TIMESTAMP, 
        '' AS ALASKA_VA,
        '' AS ALASKA_CA,
        'YES' AS SEATTLE_DIST,
        'NEW' AS CHANGE_CODE

        FROM SCDBFP10.PMVHM7PF

        LEFT JOIN SCDBFP10.PMPVNHPF
        ON M7ACAN = NHCANO
        AND M7ACAN <> 0

        WHERE M7VAGN IN ({va})

        UNION

        SELECT DISTINCT 
        '0' AS LEAD_VA, 
        CAST(NHCANO AS VARCHAR(11)) AS LEAD_CA, 
        '' AS VA_TYPE,
        '' AS VENDOR_NBR, 
        TRIM(NHCADC) AS DESCRIPTION, 
        '' AS PAST_DUE_DEDUCT, 
        NHAGTY AS CA_TYPE,  
        NHAGTP AS REBATE_TYPE, 
        NHCOBS AS COST_BASIS, 
        'F' AS ALASKA_COST_BASIS, 
        LEFT(RIGHT(NHCASD,4),2) || RIGHT(NHCASD,2) || RIGHT(LEFT(NHCASD,4),2) AS START_DT, 
        LEFT(RIGHT(NHCAED,4),2) || RIGHT(NHCAED,2) || RIGHT(LEFT(NHCAED,4),2) AS END_DT,
        NHFRQC AS BILLING_FREQ, 
        CAST(NHDYNO AS VARCHAR(5)) AS BILLING_DAY, 
        NHDOW AS BILLING_DOW, 
        '' AS BILLBACK_FORMAT, 
        '' AS PRE_APPROVAL,
        '' AS CORP_CLAIMED, 
        TRIM(NHAPNM) AS APPROP_NAME,
        '{today}' AS TIMESTAMP,
        '' AS ALASKA_VA,
        '' AS ALASKA_CA,
        'YES' AS SEATTLE_DIST,
        'NEW' AS CHANGE_CODE

        FROM SCDBFP10.PMPVNHPF

        WHERE NHCANO IN ({ca})
    ''').fetchall()

    # close sus connection
    sus.close()

    # return dataset with agreement header details
    return rows


# function to get all agreement item eligibility details from sus as240a and return list of values
def agreement_item_eligibility(agreements):

    # establish connection to sus as240a
    sus = db.sus('055')

    # clean dictionary values for use in query
    agreement_dictionary = parse_agreement_dictionary(agreements)

    # set query string for vendor and customer agreements 
    va = agreement_dictionary['va']
    ca = agreement_dictionary['ca']

    # get all agreement item eligibility details from sus as240a
    rows = sus.execute(f'''
        SELECT 
        CAST(M7VAGN AS VARCHAR(11)) AS LEAD_VA, 
        CAST(M7ACAN AS VARCHAR(11)) AS LEAD_CA, 
        TRIM(QBITEM) AS ITEM, 
        QBXVBS AS VA_REBATE_BASIS,
        CAST(CAST(ROUND(QBXVRB,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS VA_REBATE_AMT, 
        CAST(CAST(ROUND(QBXVRB,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS VA_ALASKA_REBATE_AMT, 
        CASE WHEN QBXVAV = 1 AND LEFT(QBXVBS,1) <> 'D' THEN '100' ELSE CAST(CAST(ROUND(QBXVAV,3) AS DECIMAL(10,3)) AS VARCHAR(11)) END AS VA_APPROP_AMT, 
        QXACBS AS CA_REBATE_BASIS,
        CAST(CAST(ROUND(QXXAMT,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ALLOWANCE, 
        CAST(CAST(ROUND(QXCBAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_COMM_BASE,
        CAST(CAST(ROUND(QXAPAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ADJ_AP, 
        CAST(CAST(ROUND(QXAPAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ALASKA_ADJ_AP, 
        '{today}' AS TIMESTAMP,
        '' AS SOURCE_VNDR

        FROM SCDBFP10.PMVHM7PF

        INNER JOIN SCDBFP10.PMPZQBPF
        ON M7VAGN = QBVAGN

        INNER JOIN SCDBFP10.PMPZQXPF
        ON M7ACAN = QXCANO
        AND QBITEM = QXITEM
        AND M7ACAN <> 0 

        WHERE M7VAGN IN ({va})

        UNION

        SELECT 
        CAST(M7VAGN AS VARCHAR(11)) AS LEAD_VA, 
        '0' AS LEAD_CA, 
        TRIM(QBITEM) AS ITEM, 
        QBXVBS AS VA_REBATE_BASIS,
        CAST(CAST(ROUND(QBXVRB,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS VA_REBATE_AMT, 
        CAST(CAST(ROUND(QBXVRB,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS VA_ALASKA_REBATE_AMT, 
        CASE WHEN QBXVAV = 1 AND LEFT(QBXVBS,1) <> 'D' THEN '100' ELSE CAST(CAST(ROUND(QBXVAV,3) AS DECIMAL(10,3)) AS VARCHAR(11)) END AS VA_APPROP_AMT, 
        '' AS CA_REBATE_BASIS,
        '' AS CA_ALLOWANCE, 
        '' AS CA_COMM_BASE,
        '' AS CA_ADJ_AP, 
        '' AS CA_ALASKA_ADJ_AP, 
        '{today}' AS TIMESTAMP,
        '' AS SOURCE_VNDR

        FROM SCDBFP10.PMVHM7PF

        INNER JOIN SCDBFP10.PMPZQBPF
        ON M7VAGN = QBVAGN

        WHERE M7VAGN IN ({va})
        AND M7ACAN = 0

        UNION

        SELECT 
        '0' AS LEAD_VA, 
        CAST(NHCANO AS VARCHAR(11)) AS LEAD_CA, 
        TRIM(QXITEM) AS ITEM, 
        '' AS VA_REBATE_BASIS,
        '' AS VA_REBATE_AMT, 
        '' AS VA_ALASKA_REBATE_AMT, 
        '' AS VA_APPROP_AMT, 
        QXACBS AS CA_REBATE_BASIS,
        CAST(CAST(ROUND(QXXAMT,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ALLOWANCE, 
        CAST(CAST(ROUND(QXCBAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_COMM_BASE,
        CAST(CAST(ROUND(QXAPAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ADJ_AP, 
        CAST(CAST(ROUND(QXAPAJ,3) AS DECIMAL(10,3)) AS VARCHAR(11)) AS CA_ALASKA_ADJ_AP, 
        '{today}' AS TIMESTAMP,
        '' AS SOURCE_VNDR

        FROM SCDBFP10.PMPVNHPF

        INNER JOIN SCDBFP10.PMPZQXPF
        ON NHCANO = QXCANO

        WHERE NHCANO IN ({ca})
    ''').fetchall()

    # close sus connection
    sus.close()

    # return dataset with agreement item eligibility details
    return rows


# function to pull all active items in alaska and return a list of records.
def alaska_items(dataset):

    # establish connection to sus as450a
    sus = db.sus('450')

    # get number of items to query
    row_count = len(dataset)

    # sus odbc drivers cannot handl > 10000 records in where statement - handle differently depending on size of dataset
    if row_count > 10000:

        # setup list to append query results
        all_items = []

        # loop through dataset in chuncks of 10000 records
        for i in range(0, row_count, 10000):

            # format sql string of all items on agreements loaded <yesterday>
            items = "'" + "','".join(str(row.ITEM) for row in dataset[i:i+10000]) + "'"

            # pull all customer specs with account ties in alaska
            item_chunk = sus.execute(f'''
                SELECT DISTINCT 
                TRIM(JFITEM) AS ITEM, 
                TRIM(MQPVSF) AS SOURCE_VNDR

                FROM SCDBFP10.USIAJFPF 

                LEFT JOIN SCDBFP10.USIAMQRF 
                ON JFITEM = MQITEM

                WHERE TRIM(JFITEM) IN ({items}) 
            ''').fetchall()

            # append query return to list
            all_items = all_items + item_chunk
            
    elif row_count > 0:

        # format sql string of all items on agreements loaded <yesterday>
        items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"

        # pull all customer specs with account ties in alaska
        all_items = sus.execute(f'''
            SELECT DISTINCT 
            TRIM(JFITEM) AS ITEM, 
            TRIM(MQPVSF) AS SOURCE_VNDR

            FROM SCDBFP10.USIAJFPF 

            LEFT JOIN SCDBFP10.USIAMQRF 
            ON JFITEM = MQITEM

            WHERE TRIM(JFITEM) IN ({items})
        ''').fetchall()

    # close sus connection
    sus.close()

    # return list of alaska items
    return all_items


# function to delete all lead agreements from the alaska_item_eligibility and alaska_customer_elgibility
# tables where there are no active items in alaska. return dictionary of <relevent> vendor and customer deals. 
def delete_agreements_without_items(dataset):

    # format string of items in alaska
    items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"

    # delete all lead agreements where there are no items in alaska and commit transaction
    db.sql_server.execute(f'''
        BEGIN TRANSACTION

            DELETE 

            FROM Alaska_Item_Eligibility

            WHERE ITEM NOT IN ({items})
            AND TIMESTAMP = '{today}'

            DELETE T1 

            FROM Alaska_Customer_Eligibility AS T1

            LEFT JOIN Alaska_Item_Eligibility AS T2
            ON T1.VA = T2.VA 
            AND T1.CA = T2.CA

            WHERE T2.PRIMARY_KEY IS NULL

        COMMIT
    ''')
    db.sql_server.commit()

    # get list of <relevent> vendor agreements (va <> 0)
    va = db.sql_server.execute(f'''
        SELECT DISTINCT VA
        
        FROM Alaska_Item_Eligibility
        
        WHERE VA <> 0 
        AND TIMESTAMP = '{today}'
    ''').fetchall()

    # get list of <relevent> customer agreements (ca <> 0)
    ca = db.sql_server.execute(f'''
        SELECT DISTINCT CA
        
        FROM Alaska_Item_Eligibility
        
        WHERE VA = 0
        AND TIMESTAMP = '{today}'
    ''').fetchall()

    # create dictionary containing list of vendor and customer agreements to be returned
    deviations = {
        'va':va,
        'ca':ca
    }

    # return dictionary of <relevent> vendor and customer agreements
    return deviations


# function to clear all three alaska deviation tables where timestamp is today
def delete_database_records():

    # delete all records from the three alaska deviations tables
    db.sql_server.execute(f''' 
        BEGIN TRANSACTION 

        DELETE FROM Alaska_Customer_Eligibility WHERE TIMESTAMP = '{today}'
        DELETE FROM Alaska_Item_Eligibility WHERE TIMESTAMP = '{today}'
        DELETE FROM Alaska_Header WHERE TIMESTAMP = '{today}'

        COMMIT
    ''')

    
# function to return all agreement details in sql server in dictionary format
def deviation_details():

    # get today's date
    now = date.today()

    # query all agreement header details and assign to dataset variable
    header = db.sql_server.execute(f'''
        SELECT * FROM Alaska_Header
        WHERE TIMESTAMP = '{today}' 
        OR (
            ALASKA_VA IN ('VA10023', 'VA10024')
            AND DATEFROMPARTS('20' + RIGHT(END_DT,2), LEFT(END_DT,2), RIGHT(LEFT(END_DT,4),2)) >= '{now}'
        )
        ORDER BY VA, CA
    ''').fetchall()

    # loop through each each agreement and add to respective dictionary
    item_eligibility = {}
    customer_eligibility = {}
    for agmt in header:

        # query all agreement item eligibility details and assign to dataset variable
        items = db.sql_server.execute(f'''
            SELECT * FROM Alaska_Item_Eligibility
            WHERE CONCAT(VA,CA) = '{agmt.VA + agmt.CA}' 
            ORDER BY ITEM
        ''').fetchall()

        # query all agreement customer eligibility details and assign to dataset variable
        customers = db.sql_server.execute(f'''
            SELECT * FROM Alaska_customer_Eligibility
            WHERE CONCAT(VA,CA) = '{agmt.VA + agmt.CA }' 
            ORDER BY SPEC
        ''').fetchall()

        item_eligibility[agmt.VA + agmt.CA] = items
        customer_eligibility[agmt.VA + agmt.CA] = customers

    # assemble dictionary of all agreement information and return 
    deviations = {
        'header': header, 
        'item_eligibility': item_eligibility,
        'customer_eligibility': customer_eligibility
    }  

    return deviations


# update sql server to include source vendor number for each item in alaska_item_eligibility
def update_source_vendor():

    # update source vendor number in sql server by item
    db.sql_server.execute(f'''
        UPDATE T1

        SET T1.SOURCE_VNDR = T2.SOURCE_VNDR
        
        FROM Alaska_Item_Eligibility AS T1

        INNER JOIN Item_Source_Vendor AS T2
        ON T1.ITEM = T2.ITEM

        WHERE T1.TIMESTAMP = '{today}'
    ''')

    # commit transactions and complete recordset update
    db.sql_server.commit()

    # get list of items sourced from seattle
    seattle_items = db.sql_server.execute(f'''
        SELECT DISTINCT ITEM

        FROM Alaska_Item_Eligibility

        WHERE SOURCE_VNDR = '4274'
        AND TIMESTAMP = '{today}'
    ''').fetchall()
    
    # return list of items sourced from seattle
    return seattle_items


# function to insert all items on alaska deviations into the SQL server so that the appropriate 
# handling fee can be calculated. 
def seattle_item_details(dataset):

    # establish connection to sus as055a (seattle)
    sus = db.sus('055')

    # format string of items in alaska
    items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"

    # pull items, net weight, cross weight, catch weight, and freight for items sourced from seattle
    seattle_item_details = sus.execute(f'''
        SELECT 
        TRIM(JFITEM) AS ITEM, 
        CAST(JFITNW AS VARCHAR(11)) AS NET_WEIGHT, 
        CAST(JFITGW AS VARCHAR(11)) AS GROSS_WEIGHT, 
        CAST(JFITCI AS VARCHAR(11)) AS CATCH_WEIGHT, 
        CAST(T7LAPC-T7LFOB AS VARCHAR(11)) AS FREIGHT  
        
        FROM SCDBFP10.USIAJFPF 
        
        LEFT JOIN SCDBFP10.IMMHT7PF 
        ON JFITEM = T7ITEM 
        
        WHERE TRIM(JFITEM) IN ({items})
    
    ''').fetchall()

    # close sus connection
    sus.close()
    
    # return list of seattle item details
    return seattle_item_details


# function to calculate alaska rate. The calculation is determined by the cost basis of the original (lead) agreement
# the rebate type, rebate basis, and item sourcing. for full breakdown of calculation logic, see process map.
def calculate_alaska_rate():

    # update the alaska_item_eligibility table with calculated alaska rate
    db.sql_server.execute(f'''
        BEGIN TRANSACTION 

        UPDATE T1

        SET T1.VA_ALASKA_AMT = 
        CASE 
            WHEN T3.COST_BASIS = 'D' 
            AND RIGHT(T1.VA_REBATE_BASIS, 1) <> 'P' 
            THEN CAST(T1.VA_REBATE_AMT AS FLOAT) + 0.69

            WHEN T3.COST_BASIS = 'D' 
            AND RIGHT(T1.VA_REBATE_BASIS, 1) = 'P' 
            THEN ROUND(CAST(T1.VA_REBATE_AMT AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)),3)

            WHEN T3.COST_BASIS = 'F' 
            AND RIGHT(T1.VA_REBATE_BASIS, 1) <> 'P' 
            THEN CAST(T1.VA_REBATE_AMT AS FLOAT) + 0.69 + CAST(T2.FREIGHT AS FLOAT)

            WHEN T3.COST_BASIS = 'F' 
            AND RIGHT(T1.VA_REBATE_BASIS, 1) = 'P' 
            AND T2.CATCH_WEIGHT = 'Y' 
            THEN ROUND(CAST(T1.VA_REBATE_AMT AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)) + CAST(T2.FREIGHT AS FLOAT),3)

            WHEN T3.COST_BASIS = 'F'
            AND RIGHT(T1.VA_REBATE_BASIS, 1) = 'P' 
            AND T2.CATCH_WEIGHT = 'N' 
            THEN ROUND(CAST(T1.VA_REBATE_AMT AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)) + (CAST(T2.FREIGHT AS FLOAT) / CAST(T2.NET_WEIGHT AS FLOAT)),3)

            ELSE T1.VA_REBATE_AMT
        END

        FROM Alaska_Item_Eligibility AS T1

        INNER JOIN SEATTLE_ITEMS AS T2
        ON T1.ITEM = T2.ITEM

        INNER JOIN Alaska_Header AS T3
        ON T1.VA = T3.VA
        AND T1.CA = T3.CA

        WHERE T1.VA_REBATE_AMT <> ''
        AND T1.VA_REBATE_BASIS NOT IN ('GC','GP','DC','DP') 
        AND T1.TIMESTAMP = '{today}'

        UPDATE T1

        SET T1.CA_ALASKA_ADJ_AP = 
        CASE 
            WHEN T3.COST_BASIS = 'D' 
            AND RIGHT(T1.CA_REBATE_BASIS, 1) <> 'P' 
            THEN CAST(T1.CA_ADJ_AP AS FLOAT) + 0.69

            WHEN T3.COST_BASIS = 'D' 
            AND RIGHT(T1.CA_REBATE_BASIS, 1) = 'P' 
            THEN ROUND(CAST(T1.CA_ADJ_AP AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)),3)

            WHEN T3.COST_BASIS = 'F' 
            AND RIGHT(T1.CA_REBATE_BASIS, 1) <> 'P' 
            THEN CAST(T1.CA_ADJ_AP AS FLOAT) + 0.69 + CAST(T2.FREIGHT AS FLOAT)

            WHEN T3.COST_BASIS = 'F' 
            AND RIGHT(T1.CA_REBATE_BASIS, 1) = 'P' 
            AND T2.CATCH_WEIGHT = 'Y' 
            THEN ROUND(CAST(T1.CA_ADJ_AP AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)) + CAST(T2.FREIGHT AS FLOAT),3)

            WHEN T3.COST_BASIS = 'F'
            AND RIGHT(T1.CA_REBATE_BASIS, 1) = 'P' 
            AND T2.CATCH_WEIGHT = 'N' 
            THEN ROUND(CAST(T1.CA_ADJ_AP AS FLOAT) + (0.69 / CAST(T2.GROSS_WEIGHT AS FLOAT)) + (CAST(T2.FREIGHT AS FLOAT) / CAST(T2.NET_WEIGHT AS FLOAT)),3)
        END

        FROM Alaska_Item_Eligibility AS T1

        INNER JOIN SEATTLE_ITEMS AS T2
        ON T1.ITEM = T2.ITEM

        INNER JOIN Alaska_Header AS T3
        ON T1.VA = T3.VA
        AND T1.CA = T3.CA

        WHERE T1.CA_ADJ_AP <> ''
        AND T1.CA_REBATE_BASIS NOT IN ('GC','GP','DC','DP') 
        AND T1.TIMESTAMP = '{today}'

        COMMIT
    ''')

    # commit transaction and update table
    db.sql_server.commit()


# function to update sql server with newly created agreements
def insert_alaska_agreements(primary_key, va, ca):

    # update alaska_header table with newly created agreement numbers
    db.sql_server.execute(f'''
        UPDATE Alaska_Header 

        SET ALASKA_VA = '{va}',
        ALASKA_CA = '{ca}'

        WHERE PRIMARY_KEY = {primary_key}
    ''')

    # commit transaction 
    db.sql_server.commit()


# function to update sql server with newly created agreements
def update_term_dates(va, change, end, start=None):

    # get today's date
    now = date.today()

    update_start = ''
    if start is not None: update_start = f", START_DT = '{start}'"

    # update alaska_header table with newly created agreement numbers
    db.sql_server.execute(f'''
        UPDATE Alaska_Header 

        SET END_DT = '{end}',
        TIMESTAMP = '{now}',
        CHANGE_CODE = '{change}'
        {update_start}

        WHERE ALASKA_VA = '{va}'
    ''')

    # commit transaction 
    db.sql_server.commit()


# function to delete all seattle sourcing information from sql server
def delete_seattle_records():

    # delete seattle sourcing information from item_source_vendor and seattle_items table
    db.sql_server.execute(f'''
        BEGIN TRANSACTION

        DELETE FROM Item_Source_Vendor
        DELETE FROM Seattle_Items

        COMMIT
    ''')

    # commit transaction
    db.sql_server.commit()


# function to return all vendors that either need to be pulled into alaska or require vadam ties. 
def vadam_ties():

    # query vendors for agreements that could not be created from *todays job
    vendors = db.sql_server.execute(f'''
        SELECT DISTINCT VENDOR_NBR 

        FROM Alaska_Header

        WHERE TIMESTAMP = '{today}'
        AND LEFT(ALASKA_VA,2) = 'VA' 
    ''').fetchall()

    # return list of vendors
    return vendors


# function to return all active agreements keyed by the alasak bot. 
def active_agreements():

    # get today's date
    now = date.today()

    agreements = db.sql_server.execute(f'''
        SELECT
        RIGHT('00000' + VA, 9) AS VA, 
        ALASKA_VA

        FROM ALASKA_HEADER 

        WHERE DATEFROMPARTS('20' + RIGHT(END_DT,2), LEFT(END_DT,2), RIGHT(LEFT(END_DT,4),2)) >= '{now}' 
        AND ALASKA_VA NOT IN ('', 'VA10023', 'VA10024')
    ''').fetchall()

    return agreements


# function to return active agreements keyed by the bot which have been updated yesterday 
def changed_agreements_header(agreements):

    # format active agreements for SQL statement
    active_agreements = "'" + "','".join(str(row.VA) for row in agreements) + "'"

    # establish connection to as240a
    sus = db.sus('240')

    # query agreements changed yesterday 
    changed_agreements = sus.execute(f'''
        SELECT 
        O6ENNM AS VA, 
        TRIM(O6CHDS) AS FIELD, 
        CASE 
            WHEN O6CHFN = 'M7VASD' THEN LEFT(RIGHT(M7VASD,4),2) || RIGHT(M7VASD,2) || RIGHT(LEFT(M7VASD,4),2) 
            WHEN O6CHFN = 'M7VAED' THEN LEFT(RIGHT(M7VAED,4),2) || RIGHT(M7VAED,2) || RIGHT(LEFT(M7VAED,4),2)
        END AS VALUE

        FROM SCDBFP10.PMAGO6PF

        INNER JOIN (
            SELECT 
            RIGHT('000000000' || CAST(M7VAGN AS VARCHAR(11)), 9) AS M7VAGN, 
            M7VASD, 
            M7VAED 

            FROM SCDBFP10.PMVHM7PF
        )
        ON O6ENNM = M7VAGN

        WHERE O6PMAC = 'VPD'
        AND O6EADT >= {yesterday}
        AND O6CHFN IN ('M7VASD','M7VAED')
        AND O6ENNM IN ({active_agreements})
    ''').fetchall()

    # close sus connection
    sus.close()

    return changed_agreements


# function to pull all lead agreements created <yesterday>, and store the agreements that have 
# customer specs with account ties in alaska in sql serer table alaska_customer_eligibility.
def upload_digital_deals():

    # get list of all agreements created yesterday with customer detail
    all_agreements = lead_agreements_created_yesterday()

    # the following functions require alaska records. if records are missing at any point, the function 
    # will (and should) fail. in this case, a cleanup will be run to purge today's records from the server. 
    #try:

    # insert agreement/customer eligibility dataset into sql server and return remaining items
    insert_into_sql_server(all_agreements, 'Alaska_Customer_Eligibility')

    # get list of all customer specs with account ties in alska
    alaska_specs = alaska_accounts(all_agreements)

    # delete agreements from sql server that do not have specs attached with account ties 
    # in alaska. return list of <relevent> agreement numbers
    alaska_deviations = delete_agreements_without_customers(alaska_specs)

    # get list of alaska agreement item eligibliity details from sus
    item_eligibility = agreement_item_eligibility(alaska_deviations)

    # insert agreement/item eligibility dataset into sql server
    insert_into_sql_server(item_eligibility, 'Alaska_Item_Eligibility')

    # get list of all items active alska
    valid_alaska_items = alaska_items(item_eligibility)

    # get list of agreements from sql server that do not have active items in alaska. 
    alaska_deviations = delete_agreements_without_items(valid_alaska_items)

    # insert item and source vendor details into sql server
    insert_into_sql_server(valid_alaska_items, 'Item_Source_Vendor')

    # update Alaska_Item_Eligibility with source vendor information and return list of items sourced from seattle
    seattle_items = update_source_vendor()

    # get list of alaska item details sourced from seattle
    seattle_sourcing = seattle_item_details(seattle_items)

    # insert seattle item detail into sql server
    insert_into_sql_server(seattle_sourcing, 'Seattle_Items')

    # get list of alaska agreement header details from sql servr
    header_details = agreement_header(alaska_deviations)

    # insert agreement header dataset into sql server
    insert_into_sql_server(header_details, 'Alaska_Header')

    # calculate alska rate in sql server
    calculate_alaska_rate()

    # delete all item sourcing information from sql server
    delete_seattle_records()

    # remove zoned agreements where seattle is not in distribution list
    #clear_zoned_agreements()

    # get dictionary of agreement criteria for return. dictionary to include agreement header, item, and customer detail
    #deviations = deviation_details()

    # print successful message and return dictionary
    print('digital deals upload complete')
    return
    #except:

        # clear datase of partial records. the reason for this exception is that all deviations loaded 
        # <yesterday> either have no active items in alaska, or no active customer ties. 
        #delete_database_records()
        #print('no deviatinos to upload')


def get_updated_agreements():

    # get list of all active lead agreement w/ local agreement keyed by alaska. 
    bot_agreements = active_agreements()

    # dictionary of all agreements where key is lead va and item is alaska va
    agreement_reference = {agreement[0]:agreement[1] for agreement in bot_agreements}

    # get list of updated agreements  
    updated_agreements = changed_agreements_header(bot_agreements)

    # create dictionary of updated agreements using the alaska va as the key
    compiled_updates = {}
    for agreement in updated_agreements:
        
        va = agreement_reference[agreement.VA]
        field = agreement.FIELD
        value = str(agreement.VALUE)

        if va not in compiled_updates: compiled_updates[va] = {}

        compiled_updates[va][field] = value

    return compiled_updates

