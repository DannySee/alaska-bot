from datetime import datetime, timedelta, date

# define formatted date parameters: yesterday = sus format, today = timestamp format
timestamp = datetime.now().strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y%m%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')

dpm_agreement_customers = f'''
    SELECT 
    CAST(M7VAGN AS VARCHAR(11)) AS VA, 
    CAST(M7ACAN AS VARCHAR(11)) AS CA, 
    AZPCIE AS IEA, 
    AZPCSC AS SPEC_CODE, 
    TRIM(AZPCSP) AS SPEC, 
    LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
    LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
    '{timestamp}' AS TIMESTAMP,
    'VA' AS ELIGIBILITY_TYPE, 
    'DPM' AS ORIGINATOR

    FROM SCDBFP10.PMVHM7PF 

    INNER JOIN SCDBFP10.USCNAZL0 
    ON RIGHT('000' || M7VAGN, 9) = AZCEEN 
    AND AZCEAI = 'VA ' 

    LEFT JOIN SCDBFP10.PMDPDVRF
    ON CAST(M7VAGN AS VARCHAR(11)) = TRIM(DVCPM9)
    AND TRIM(DVCPTY) = 'VA'

    WHERE M7EADT = {yesterday}
    AND M7ACAN = 0
    AND M7PPAF = 'PD'
    AND M7AGTY <> 'NGEL'
    AND M7VAGD NOT LIKE '%VOID%' 
    AND M7VAGD NOT LIKE '%RBB%'
    AND (
        LENGTH(TRIM(DVPDDA)) <> 3 
        OR UPPER(DVPDDA) <> LOWER(DVPDDA) 
        OR TRIM(DVPDDA) = '055'
    )
    AND TRIM(DVT500) NOT LIKE '%450'

    UNION

    SELECT 
    CAST(M7VAGN AS VARCHAR(11)) AS VA, 
    CAST(M7ACAN AS VARCHAR(11)) AS CA, 
    T1.AZPCIE AS IEA, 
    T1.AZPCSC AS SPEC_CODE, 
    TRIM(T1.AZPCSP) AS SPEC, 
    LEFT(RIGHT(T1.AZEFSD,4),2) || RIGHT(T1.AZEFSD,2) || RIGHT(LEFT(T1.AZEFSD,4),2) AS START_DT, 
    LEFT(RIGHT(T1.AZEFED,4),2) || RIGHT(T1.AZEFED,2) || RIGHT(LEFT(T1.AZEFED,4),2) AS END_DT,
    '{timestamp}' AS TIMESTAMP,
    CASE WHEN T2.AZCEEN IS NULL THEN 'CA' ELSE 'VA|CA' END AS ELIGIBILITY_TYPE, 
    'DPM' AS ORIGINATOR

    FROM SCDBFP10.PMVHM7PF 

    INNER JOIN SCDBFP10.USCNAZL0 AS T1
    ON RIGHT('000' || M7ACAN, 9) = T1.AZCEEN 
    AND T1.AZCEAI = 'CA ' 

    LEFT JOIN  SCDBFP10.USCNAZL0 AS T2
    ON RIGHT('000' || M7VAGN, 9) = T2.AZCEEN 
    AND T2.AZCEAI = 'VA ' 
    AND T1.AZPCSP = T2.AZPCSP

    LEFT JOIN SCDBFP10.PMDPDVRF
    ON CAST(M7VAGN AS VARCHAR(11)) = TRIM(DVCPM9)
    AND TRIM(DVCPTY) = 'VA'

    WHERE M7EADT = {yesterday}
    AND M7ACAN <> 0
    AND M7PPAF = 'PD'
    AND M7AGTY <> 'NGEL'
    AND M7VAGD NOT LIKE '%VOID%' 
    AND M7VAGD NOT LIKE '%RBB%'
    AND (
        LENGTH(TRIM(DVPDDA)) <> 3 
        OR UPPER(DVPDDA) <> LOWER(DVPDDA) 
        OR TRIM(DVPDDA) = '055'
    )
    AND TRIM(DVT500) NOT LIKE '%450'

    UNION

    SELECT '0' AS VA, 
    CAST(NHCANO AS VARCHAR(11)) AS CA, 
    AZPCIE AS IEA, 
    AZPCSC AS SPEC_CODE,
    TRIM(AZPCSP) AS SPEC, 
    LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
    LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
    '{timestamp}' AS TIMESTAMP,
    'CA' AS ELIGIBILITY_TYPE, 
    'DPM' AS ORIGINATOR

    FROM SCDBFP10.PMPVNHPF

    INNER JOIN SCDBFP10.USCNAZL0 
    ON RIGHT('000' || NHCANO, 9) = AZCEEN 
    AND AZCEAI = 'CA ' 

    LEFT JOIN SCDBFP10.PMDPDVRF
    ON CAST(NHCANO AS VARCHAR(11)) = TRIM(DVCPM9)
    AND TRIM(DVCPTY) = 'CA'

    WHERE NHEADT = {yesterday}
    AND NHCVAN = 0
    AND NHPPAF = 'PD'
    AND NHAGTY <> 'NGEL'
    AND NHCADC NOT LIKE '%VOID%' 
    AND NHCADC NOT LIKE '%RBB%'
    AND (
        LENGTH(TRIM(DVPDDA)) <> 3 
        OR UPPER(DVPDDA) <> LOWER(DVPDDA) 
        OR TRIM(DVPDDA) = '055'
    )
    AND TRIM(DVT500) NOT LIKE '%450'
'''

dgd_agreement_customers = f'''
    SELECT 
    CAST(M7VAGN AS VARCHAR(11)) AS VA, 
    CAST(M7ACAN AS VARCHAR(11)) AS CA, 
    AZPCIE AS IEA, 
    AZPCSC AS SPEC_CODE, 
    TRIM(AZPCSP) AS SPEC, 
    LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
    LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
    '{timestamp}' AS TIMESTAMP,
    'VA' AS ELIGIBILITY_TYPE, 
    'DGD' AS ORIGINATOR

    FROM SCDBFP10.PMVHM7PF 

    INNER JOIN SCDBFP10.USCNAZL0 
    ON RIGHT('000' || M7VAGN, 9) = AZCEEN 
    AND AZCEAI = 'VA ' 

    WHERE M7EADT = {yesterday}
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
    '{timestamp}' AS TIMESTAMP,
    CASE WHEN T2.AZCEEN IS NULL THEN 'CA' ELSE 'VA|CA' END AS ELIGIBILITY_TYPE, 
    'DGD' AS ORIGINATOR

    FROM SCDBFP10.PMVHM7PF 

    INNER JOIN SCDBFP10.USCNAZL0 AS T1
    ON RIGHT('000' || M7ACAN, 9) = T1.AZCEEN 
    AND T1.AZCEAI = 'CA ' 

    LEFT JOIN  SCDBFP10.USCNAZL0 AS T2
    ON RIGHT('000' || M7VAGN, 9) = T2.AZCEEN 
    AND T2.AZCEAI = 'VA ' 
    AND T1.AZPCSP = T2.AZPCSP

    WHERE M7EADT = {yesterday}
    AND M7ACAN <> 0
    AND M7VAGD NOT LIKE '%VOID%' 
    AND M7AGRN = 999999999

    UNION

    SELECT '0' AS VA, 
    CAST(NHCANO AS VARCHAR(11)) AS CA, 
    AZPCIE AS IEA, 
    AZPCSC AS SPEC_CODE,
    TRIM(AZPCSP) AS SPEC, 
    LEFT(RIGHT(AZEFSD,4),2) || RIGHT(AZEFSD,2) || RIGHT(LEFT(AZEFSD,4),2) AS START_DT, 
    LEFT(RIGHT(AZEFED,4),2) || RIGHT(AZEFED,2) || RIGHT(LEFT(AZEFED,4),2) AS END_DT,
    '{timestamp}' AS TIMESTAMP,
    'CA' AS ELIGIBILITY_TYPE, 
    'DGD' AS ORIGINATOR

    FROM SCDBFP10.PMPVNHPF

    INNER JOIN SCDBFP10.USCNAZL0 
    ON RIGHT('000' || NHCANO, 9) = AZCEEN 
    AND AZCEAI = 'CA ' 

    WHERE NHEADT = {yesterday}
    AND NHCVAN = 0
    AND NHCADC NOT LIKE '%VOID%' 
    AND NHAGRN = 999999999
'''

database_cleanup = f'''
    BEGIN TRANSACTION 

        DELETE FROM Alaska_Customer_Eligibility WHERE TIMESTAMP = '{timestamp}'
        DELETE FROM Alaska_Item_Eligibility WHERE TIMESTAMP = '{timestamp}'
        DELETE FROM Alaska_Header WHERE TIMESTAMP = '{timestamp}'

    COMMIT
'''

update_item_rates = f'''
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
        AND T1.TIMESTAMP = '{timestamp}'

    COMMIT
'''

delete_seattle_records = f'''
    BEGIN TRANSACTION
        DELETE FROM Item_Source_Vendor
        DELETE FROM Seattle_Items
    COMMIT
'''

get_zoned_agreements = f'''
    SELECT DISTINCT CONCAT(HEADER.VA,HEADER.CA) AS ZONED

    FROM (
        SELECT 
        HEADER.VA, 
        HEADER.CA, 
        ITEM.ITEM, 
        CUSTOMER.SPEC, 
        HEADER.SEATTLE_DIST 

        FROM ALASKA_HEADER AS HEADER

        INNER JOIN Alaska_Customer_Eligibility AS CUSTOMER
        ON HEADER.VA = CUSTOMER.VA
        AND HEADER.CA = CUSTOMER.CA

        INNER JOIN Alaska_Item_Eligibility as ITEM
        ON HEADER.VA = ITEM.VA 
        AND HEADER.CA = ITEM.CA

        WHERE HEADER.TIMESTAMP = '{timestamp}'
    ) AS PARENT

    INNER JOIN Alaska_Customer_Eligibility AS CUSTOMER 
    ON PARENT.SPEC = CUSTOMER.SPEC
    AND CONCAT(PARENT.VA, PARENT.CA) <> CONCAT(CUSTOMER.VA, CUSTOMER.CA)
    AND CUSTOMER.TIMESTAMP = '{timestamp}'

    INNER JOIN Alaska_ITEM_Eligibility AS ITEM 
    ON PARENT.ITEM = ITEM.ITEM
    AND CUSTOMER.VA = ITEM.VA
    AND CUSTOMER.CA = ITEM.CA
    AND ITEM.TIMESTAMP = '{timestamp}'

    INNER JOIN Alaska_Header AS HEADER
    ON CUSTOMER.VA = HEADER.VA
    AND CUSTOMER.CA = HEADER.CA
    AND HEADER.TIMESTAMP = '{timestamp}'

    WHERE HEADER.SEATTLE_DIST = 'NO'
'''


def agreement_numbers(table, type):
    operator = '<>' if type == 'VA' else '='
    return f"SELECT DISTINCT {type} FROM {table} WHERE VA {operator} 0 AND TIMESTAMP = '{timestamp}'"


def account_ties(specs):
    return f'''
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
    '''


def alaska_specs(specs):
    return f'''
        SELECT DISTINCT TRIM(JUCEEN) AS SPEC

        FROM SCDBFP10.USCLJUPF 

        WHERE TRIM(JUCEEN) IN ({specs}) 
    
        UNION 

        SELECT DISTINCT 
        TRIM(JTFPAR) AS SPEC

        FROM SCDBFP10.USCKJTPF 
        
        WHERE JTFTYP IN ('PRNT', 'MSTR') 
        AND TRIM(JTFPAR) IN ({specs}) 
    '''


def alaska_customer_cleanup(active_specs, account_ties):
    return f'''
        BEGIN TRANSACTION

            DELETE 
            
            FROM Alaska_Customer_Eligibility
            
            WHERE SPEC NOT IN ({active_specs})
            AND TIMESTAMP = '{timestamp}'

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
            AND TIMESTAMP = '{timestamp}'

        COMMIT
    '''


def alaska_item_cleanup(items):
    return f'''
    BEGIN TRANSACTION

        DELETE 

        FROM Alaska_Item_Eligibility

        WHERE ITEM NOT IN ({items})
        AND TIMESTAMP = '{timestamp}'

        DELETE T1 

        FROM Alaska_Customer_Eligibility AS T1

        LEFT JOIN Alaska_Item_Eligibility AS T2
        ON T1.VA = T2.VA 
        AND T1.CA = T2.CA

        WHERE T2.PRIMARY_KEY IS NULL

    COMMIT
    '''


def dpm_agreement_header(va, ca):
    return f'''
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
        '{timestamp}' AS TIMESTAMP, 
        '' AS ALASKA_VA,
        '' AS ALASKA_CA,
        CASE WHEN DVT500 LIKE '%055%' THEN 'YES' ELSE 'NO' END AS SEATTLE_DIST,
        'NEW' AS CHANGE_CODE

        FROM SCDBFP10.PMVHM7PF

        LEFT JOIN SCDBFP10.PMPVNHPF
        ON M7ACAN = NHCANO
        AND M7ACAN <> 0

        LEFT JOIN SCDBFP10.PMDPDVRF
        ON CAST(M7VAGN AS VARCHAR(11)) = TRIM(DVCPM9)
        AND TRIM(DVCPTY) = 'VA'

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
        '{timestamp}' AS TIMESTAMP,
        '' AS ALASKA_VA,
        '' AS ALASKA_CA,
        CASE WHEN DVT500 LIKE '%055%' THEN 'YES' ELSE 'NO' END AS SEATTLE_DIST,
        'NEW' AS CHANGE_CODE

        FROM SCDBFP10.PMPVNHPF

        LEFT JOIN SCDBFP10.PMDPDVRF
        ON CAST(NHCANO AS VARCHAR(11)) = TRIM(DVCPM9)
        AND TRIM(DVCPTY) <> 'VA'

        WHERE NHCANO IN ({ca})
    '''


def dgd_agreement_header(va, ca):
    return f'''
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
        '{timestamp}' AS TIMESTAMP, 
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
        '{timestamp}' AS TIMESTAMP,
        '' AS ALASKA_VA,
        '' AS ALASKA_CA,
        'YES' AS SEATTLE_DIST,
        'NEW' AS CHANGE_CODE

        FROM SCDBFP10.PMPVNHPF

        WHERE NHCANO IN ({ca})
    '''

def usbl_agreement_item(va, ca):
    return f'''
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
        '{timestamp}' AS TIMESTAMP,
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
        '{timestamp}' AS TIMESTAMP,
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
        '{timestamp}' AS TIMESTAMP,
        '' AS SOURCE_VNDR

        FROM SCDBFP10.PMPVNHPF

        INNER JOIN SCDBFP10.PMPZQXPF
        ON NHCANO = QXCANO

        WHERE NHCANO IN ({ca})
    '''

def valid_alaska_items(items):
    return f'''
        SELECT DISTINCT 
        TRIM(JFITEM) AS ITEM, 
        TRIM(MQPVSF) AS SOURCE_VNDR

        FROM SCDBFP10.USIAJFPF 

        LEFT JOIN SCDBFP10.USIAMQRF 
        ON JFITEM = MQITEM

        WHERE TRIM(JFITEM) IN ({items}) 
    '''

def seattle_item_info(items):
    return f'''
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
    '''


def log_alaska_agreement(primary_key, va, ca):
    return f'''
        UPDATE Alaska_Header 
        SET ALASKA_VA = '{va}', 
        ALASKA_CA = '{ca}' 
        WHERE PRIMARY_KEY = {primary_key}'
    '''


def update_term_dates(va, change, end, start=None):

    update_start = '' if start is None else f", START_DT = '{start}'" 
    return f'''
        UPDATE Alaska_Header 
        SET END_DT = '{end}',
        TIMESTAMP = '{timestamp}',
        CHANGE_CODE = '{change}'
        {update_start}

        WHERE ALASKA_VA = '{va}'
    '''
       

def delete_zoned_agreements(agreements):
    return f'''
        BEGIN TRANSACTION
            DELETE FROM ALASKA_HEADER WHERE CONCAT(VA, CA) IN ({agreements})
            DELETE FROM ALASKA_ITEM_ELIGIBILITY WHERE CONCAT(VA, CA) IN ({agreements})
            DELETE FROM ALASKA_CUSTOMER_ELIGIBILITY WHERE CONCAT(VA, CA) IN ({agreements})
        COMMIT
    '''