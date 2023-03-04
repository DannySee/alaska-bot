import database_manager as db
from datetime import datetime, timedelta

sql_server = db.sql_server

today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')


def lead_agreements_customer_detail():
    sus = db.sus('240')

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
    ''').fetchall()

    rowCount = len(rows)

    if rowCount > 1000:
        for i in range(0, rowCount, 1000):
            dataset = ','.join(str(row) for row in rows[i:i+1000])
            sql_server.execute(f'INSERT INTO Alaska_Customer_Eligibility VALUES{dataset}')
            
        sql_server.commit()
    else:
        dataset = ','.join(str(row) for row in rows)
        
        if dataset != '':
            sql_server.execute(f'INSERT INTO Alaska_Customer_Eligibility VALUES{dataset}')
            sql_server.commit()

    rows = sql_server.execute(f'''
        SELECT DISTINCT SPEC

        FROM Alaska_Customer_Eligibility

        WHERE TIMESTAMP = '{today}'
        AND IEA <> 'E'
    ''').fetchall()

    dataset = "'" + "','".join(str(row.SPEC) for row in rows) + "'"
    
    sus = db.sus('450')

    rows = sus.execute(f'''
        SELECT DISTINCT TRIM(AZCEEN) AS SPEC

        FROM SCDBFP10.USCNAZL0 

        WHERE TRIM(AZCEEN) IN ({dataset}) 
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
        WHERE TRIM(T1.JTTPAR) IN ({dataset}) 
        OR TRIM(T2.JTTPAR) IN ({dataset})
    ''').fetchall()

    dataset = "'" + "','".join(str(row.SPEC) for row in rows) + "'"

    sql_server.execute(f'''
        DELETE 
        
        FROM Alaska_Customer_Eligibility
        
        WHERE VA NOT IN (
            SELECT VA 

            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({dataset})
            AND VA <> '0'
        )
        AND CA NOT IN (
            SELECT CA 
            
            FROM Alaska_Customer_Eligibility

            WHERE SPEC IN ({dataset})
            AND CA <> '0'
        )
        AND TIMESTAMP = '{today}'
    ''')
    sql_server.commit()

    

lead_agreements_customer_detail()

