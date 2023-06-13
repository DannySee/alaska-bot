import sql
import data_centers as db

from datetime import datetime, timedelta, date

# define formatted date parameters: yesterday = sus format, today = timestamp format
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')


# pull all pricing agreements keyed day prior with customer detail out of 240 (dpm) 055 (Digital Deals)
def usbl_agreements_created_yesterday():

    # pull all dpm pricing agreements keyed day prior with customer detail out of 240
    sus = db.sus('240')
    customer_eligibility = sus.execute(sql.dpm_agreement_customers).fetchall()
    sus.close()

    # pull all pricing agreements keyed day prior with customer detail out of sus
    sus = db.sus('055')
    customer_eligibility.extend(sus.execute(sql.dgd_agreement_customers).fetchall())
    sus.close()

    # return list of values
    return customer_eligibility


# insert dataset into sql server. dataset must have same formatting and data fields as server table
def insert_into_sql_server(dataset, table):

    # establish connection to sql server
    sql_server =- db.sql_server()

    # sql server has an insert row max of 1000 records - handle differently depending on size of dataset
    row_count = len(dataset)
    if row_count > 1000:

        # loop through dataset in chuncks of 1000 records
        for i in range(0, row_count, 1000):

            # format chunk and insert into sql server
            rows = ','.join(str(row) for row in dataset[i:i+1000])
            sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
            
        sql_server.commit()
    elif row_count > 0:

        # format dataset, insert into sql server and commit
        rows = ','.join(str(row) for row in dataset)
        sql_server.execute(f'INSERT INTO {table} VALUES{rows}')
        sql_server.commit()

    sql_server.close()


# pull all available customer specs and account ties in alaska and return a list of records.
def alaska_accounts(dataset):

    # format sql string of all customer specs on agreements loaded <yesterday>
    specs = "'" + "','".join(str(row.SPEC) for row in dataset) + "'"

    # establish connection to sus as450a
    sus = db.sus('450')

    # pull all availalble customer specs and with account ties in alaska
    active_ties = sus.execute(sql.account_ties(specs)).fetchall()
    active_specs = sus.execute(sql.alaska_specs(specs)).fetchall()

    # compile dictionary of active ties and active specs
    customers = {
        'account_ties':active_ties,
        'active_specs':active_specs
    }

    sus.close()

    return customers


# delete all agreements from the alaska_cutomer_eligibility table where there are no active 
# customer ties/available customer specs in alaska. return dictionary of vendor and customer deals. 
def delete_agreements_without_customers(dataset):

    # establish connection to sql server
    sql_server =- db.sql_server()

    # format string of customer specs which do have ties in alaska
    account_ties = "'" + "','".join(str(row.SPEC) for row in dataset['account_ties']) + "'"
    active_specs = "'" + "','".join(str(row.SPEC) for row in dataset['active_specs']) + "'"

    # delete all lead agreements where there are no customer specs with account ties in alaska 
    sql_server.execute(sql.alaska_customer_cleanup(active_specs, account_ties))
    sql_server.commit()

    # get list of <relevent> vendor agreements (va <> 0) and create dictionary
    va = sql_server.execute(sql.agreement_numbers('Alaska_Customer_Eligibility', 'VA')).fetchall()
    ca = sql_server.execute(sql.agreement_numbers('Alaska_Customer_Eligibility', 'CA')).fetchall()
    deviations = {
        'va':va,
        'ca':ca
    }

    sql_server.close()

    return deviations


# parse dictionary to format values of dictionary items with va/ca keys into sql string
def parse_agreement_dictionary(agreements):

    # parse dictionary into sql strings
    for key in agreements:

        # if either va or ca key has no items then assign 404 so the query does not fail
        if len(agreements[key]) > 0:
            agreements[key] = ",".join(str(row[0]) for row in agreements[key]) 
        else:
            agreements[key] = '404'

    return agreements


# get all agreement header details from sus as240a and return list of values
def agreement_header(agreements):

    # clean dictionary values for use in query
    agreement_dictionary = parse_agreement_dictionary(agreements)

    # set query string for vendor and customer agreements 
    va = agreement_dictionary['va']
    ca = agreement_dictionary['ca']

    # get all agreement header details for dpm agreements
    sus = db.sus('240')
    header_details = sus.execute(sql.dpm_agreement_header(va, ca)).fetchall()
    sus.close()

    # get all agreement header details for dgd agreements
    sus = db.sus('055')
    header_details.extend(sus.execute(sql.dgd_agreement_header(va, ca)).fetchall())
    sus.close()

    return header_details


# get all agreement item eligibility details 
def agreement_item_eligibility(agreements):

    # clean dictionary values for use in query
    agreement_dictionary = parse_agreement_dictionary(agreements)

    # set query string for vendor and customer agreements 
    va = agreement_dictionary['va']
    ca = agreement_dictionary['ca']

    # get item eligibility from dpm agreements
    sus = db.sus('240')
    item_eligibility = sus.execute(sql.usbl_agreement_item(va, ca)).fetchall()
    sus.close

    # get item eligibility from dgd agreements
    sus = db.sus('055')
    item_eligibility.extend(sus.execute(sql.usbl_agreement_item(va, ca)).fetchall())
    sus.close

    return item_eligibility


# pull all active items in alaska and return a list of records.
def alaska_items(dataset):

    sus = db.sus('450')
    all_items = []

    # sus odbc drivers cannot handle > 10000 records in parameter 
    # handle differently depending on size of dataset
    row_count = len(dataset)
    if row_count > 10000:

        # loop through dataset in chuncks of 10000 records
        for i in range(0, row_count, 10000):

            # pull items available in alaska
            items = "'" + "','".join(str(row.ITEM) for row in dataset[i:i+10000]) + "'"
            item_chunk = sus.execute(sql.valid_alaska_items(items)).fetchall()
            all_items.extend(item_chunk)

    elif row_count > 0:

        # pull items available in alaska
        items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"
        all_items = sus.execute(sql.valid_alaska_items(items)).fetchall()

    sus.close()

    return all_items


# delete all agreements from the alaska_item_eligibility and alaska_customer_elgibility 
# tables where there are no active items in alaska
def delete_agreements_without_items(dataset):

    # delete all lead agreements where there are no items in alaska and commit transaction
    items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"

    sql_server = db.sql_server()
    sql_server.execute(sql.alaska_item_cleanup(items))
    sql_server.commit()

    # get list of vendor/customer agreement numbers from item table
    va = sql_server.execute(sql.agreement_numbers('Alaska_Item_Eligibility', 'VA')).fetchall()
    ca = sql_server.execute(sql.agreement_numbers('Alaska_Item_Eligibility', 'CA')).fetchall()
    deviations = {
        'va':va,
        'ca':ca
    }

    sql_server.close()
    
    return deviations


# get agreement details for each databse table
def deviation_details():

    sql_server = db.sql_server()

    # get all agreement header details
    header = sql_server.execute(sql.alaska_header_detail).fetchall()

    # loop through each each agreement and add to respective dictionary
    item_eligibility = {}
    customer_eligibility = {}
    for component in header:

        # get all agreement item/customer eligibility details 
        items = sql_server.execute(sql.alaska_item_detail).fetchall()
        customers = sql_server.execute(sql.alaska_customer_detail).fetchall()

        # add agreement records to dictionary 
        item_eligibility[component.VA + component.CA] = items
        customer_eligibility[component.VA + component.CA] = customers

    # assemble dictionary of all agreement information 
    deviations = {
        'header': header, 
        'item_eligibility': item_eligibility,
        'customer_eligibility': customer_eligibility
    }  

    sql_server.close()

    return deviations


# update sql server to include source vendor number for each item in alaska_item_eligibility
def update_source_vendor():

    sql_server = db.sql_server()

    # update source vendor number in sql server by item
    sql_server.execute(sql.update_item_sourcing)
    sql_server.commit()

    # get list of items sourced from seattle
    seattle_items = sql_server.execute(sql.seattle_sourced_items).fetchall()

    sql_server.close()
    
    return seattle_items


# execute a command to sql server database - no return
def database_command(command):
    sql_server = db.sql_server()

    # loop through and execute list of commands or execute single command
    if type(command) is list:
        [sql_server.execute(query) for query in command]
    else:
        sql_server.execute(command)

    sql_server.commit()
    sql_server.close


# insert item details into the SQL server so the handling fee can be calculated. 
def seattle_item_details(dataset):

    # pull items, net weight, gross weight, catch weight, and freight for items sourced from seattle
    sus = db.sus('055')
    items = "'" + "','".join(str(row.ITEM) for row in dataset) + "'"
    seattle_item_details = sus.execute(sql.seattle_item_info(items)).fetchall()
    sus.close()
    
    return seattle_item_details


# function to clear zoned agreements from sql server. Look at agreements with matching items
# customers and overlapping date range - remove agreement where seattle is not in distribution list.
def clear_zoned_agreements():

    sql_server = db.sql_server()

    # get zoned agreements from database
    zoned_agreements = sql_server.execute(sql.get_zoned_agreements).fetchall()

    if len(zoned_agreements) > 0:

        # remove all zoned agreements from database
        zoned = "'" + "','".join(str(row.ZONED) for row in zoned_agreements) + "'"
        sql_server.execute(sql.delete_zoned_agreements(zoned))
        sql_server.commit()

    sql_server.close()







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
        AND O6EADT >= 20230401
        AND O6CHFN IN ('M7VASD','M7VAED')
        AND O6ENNM IN ({active_agreements})
    ''').fetchall()

    # close sus connection
    sus.close()

    return changed_agreements


# upload dpm/dgd agreement customer eligibility for deals created yesterday w/ alaska customers
def alaska_customer_upload():

    alaska_deviations = {}

    # get list of all agreements created yesterday with customer detail
    all_agreements = usbl_agreements_created_yesterday()

    if len(all_agreements) > 0:

        # insert agreement customer eligibility dataset into sql server 
        insert_into_sql_server(all_agreements, 'Alaska_Customer_Eligibility')

        # get list of all customer specs with account ties in alska
        alaska_specs = alaska_accounts(all_agreements)

        # delete agreements from sql server that do not have specs with account ties in alaska. 
        # return list of alaska agreement numbers
        alaska_deviations = delete_agreements_without_customers(alaska_specs)

    return alaska_deviations


# calculate handling fee/freight upcharge and add to item rate. 
def alaska_sourcing_upload(alaska_items):

    # insert item and source vendor details into sql server
    insert_into_sql_server(alaska_items, 'Item_Source_Vendor')

    # update sql server with source vendor information and return list of items sourced from seattle
    seattle_items = update_source_vendor()

    # get list details (catch weight, net/gross weight, freight) on items sourced from seattle
    seattle_sourcing = seattle_item_details(seattle_items)

    # insert seattle item detail into sql server
    insert_into_sql_server(seattle_sourcing, 'Seattle_Items')


# upload dpm/dgd agreement item eligibility for deals created yesterday w/ alaska customers.
# update rates with handling fee/freight upcharge.
def alaska_item_upload(alaska_deviations):

    alaska_deviations = {}

    # get list of alaska agreement item eligibliity
    item_eligibility = agreement_item_eligibility(alaska_deviations)

    if len(item_eligibility) > 0:

        # insert agreement item eligibility dataset into sql server
        insert_into_sql_server(item_eligibility, 'Alaska_Item_Eligibility')

        # get list of all items available in alaska
        valid_alaska_items = alaska_items(item_eligibility)

        # upload item sourcing information in sql server
        alaska_sourcing_upload(valid_alaska_items)

        # delete agreements from sql server that do not have active itemsin alaska. 
        # return list of alaska agreement numbers
        alaska_deviations = delete_agreements_without_items(valid_alaska_items)

        # calculate handling fee/freight upcharge and clean sourcing information
        database_command([sql.update_item_rates, sql.delete_seattle_records])

    return alaska_deviations


# upload dpm/dgd agreement header details for deals created yesterday w/ alaska items/customers
# remove zoned pricing that should not be distributed to alaska
def alaska_header_upload(alaska_deviations):

    # get list of alaska agreement header details from sql servr
    header_details = agreement_header(alaska_deviations)

    # insert agreement header dataset into sql server
    insert_into_sql_server(header_details, 'Alaska_Header')

    # remove zoned agreements where seattle is not in distribution list
    clear_zoned_agreements()


# function to pull all dpm/dgd agreement elements for anything created <yesterday> with alaska eligibility
def upload_alaska_deviations():

    # get list of dpm/dgd agreement numbers for deals created yesterday w/ alaska customers
    alaska_deviations_customer = alaska_customer_upload()

    # get list of dpm/dgd agreement numbers for deals created yesterday w/ items available in alaska
    if alaska_deviations_customer: alaska_deviations_item = alaska_item_upload(alaska_deviations_customer)

    # upload alaska agreement header details 
    if alaska_deviations_item: alaska_header_upload(alaska_deviations_item)
    
    # get dictionary of agreement criteria for return. dictionary to include agreement header, item, and customer detail
    deviations = deviation_details()

    # print staus message and clean tables (item/customer) if there are no alaska devitions to create
    record_count = len(deviations['header'])
    if record_count > 0:
        database_command(sql.database_cleanup)
        print(f'upload complete: {record_count} deviations')
    else:
        print('upload complete: no deviations')

    return deviations


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

