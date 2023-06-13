import sql
import session_manager as bz

from data_pull import upload_alaska_deviations, database_command, database_query, get_updated_agreements
from outlook import send_vadam_request

# open a global bluezone session
session = bz.connect()


def load_vendor_agreement(header, item_elig, customer_elig):

    # go to quick access va
    bz.quick_access_va(session)

    # create new agreement
    va = bz.create_va(session, header.VA_TYPE, header.VENDOR_NBR)

    # key geenral agreement informatoin screen
    bz.va_general_agreement_information(
        session, 
        header.DESCRIPTION, 
        header.PAST_DUE_DEDUCT, 
        header.ALASKA_COST_BASIS, 
        header.CA_TYPE, 
        header.REBATE_TYPE, 
        header.START_DT, 
        header.END_DT
    )

    # key billing information screen
    bz.va_billing_information(
        session, 
        header.BILLING_FREQ, 
        header.BILLING_DAY, 
        header.BILLBACK_FORMAT, 
        header.PRE_APPROVAL, 
        header.CORP_CLAIMED, 
        header.APPROP_NAME
    )

    # key va item eligibility
    bz.va_item_eligibility(session, item_elig)

    # key customer eligibility if billbacks are allowed
    bz.va_customer_eligibility(session, customer_elig)

    # continue through agreement creation if there is a customer side of the agreement then 
    ca = '0'
    if header.CA_TYPE != '':
        
        # key ca item eligibility
        bz.ca_item_eligibility(session, item_elig)

        # key ca customer eligibility return ca number
        ca = bz.ca_customer_eligibility(session, customer_elig)

    # return agreement numbers
    return {'VA':va, 'CA':ca}


def load_customer_agreement(header, item_elig, customer_elig):

    # go to quick access va
    bz.quick_access_ca(session)

    # create new agreement
    ca = bz.create_ca(session, header.CA_TYPE, header.REBATE_TYPE)

    # key geenral agreement informatoin screen
    bz.ca_general_agreement_information(
        session, 
        header.DESCRIPTION, 
        header.BILLING_FREQ, 
        header.BILLING_DAY, 
        header.APPROP_NAME, 
        header.START_DT, 
        header.END_DT
    )

    # key va item eligibility
    bz.ca_item_eligibility(session, item_elig)

    # key customer eligibility if billbacks are allowed
    bz.ca_customer_eligibility(session, customer_elig)
        
    # key ca item eligibility
    bz.ca_item_eligibility(session, item_elig)

    # key ca customer eligibility return ca number
    bz.ca_customer_eligibility(session, customer_elig)

    # return agreement numbers
    return {'VA':'0', 'CA':ca}


def load_alaska_agreements():   

    # load list of alaska deviations 
    deviations = upload_alaska_deviations()
    all_header = deviations['header']
    all_item_elig = deviations['item_eligibility']
    all_customer_elig = deviations['customer_eligibility']
    agreements = []

    # loop through header list - one iteration per agreement
    for header in all_header:

        lead_va = header.VA
        lead_ca = header.CA
        id = lead_va + lead_ca
        item_elig = all_item_elig[id]
        customer_elig = all_customer_elig[id]
        
        # handle vendor and customer agreements differently
        if lead_va != '0':
            agreements.append(load_vendor_agreement(header, item_elig, customer_elig))
        else:
            agreements.append(load_customer_agreement(header, item_elig, customer_elig))
            
        # update (F7) agreement 
        bz.commit_transaction(session)

        # send alaska agreements to server
        alaska_va = agreements[-1]['VA']
        alaska_ca = agreements[-1]['CA']
        database_command(sql.log_alaska_agreement(header.PRIMARY_KEY, alaska_va, alaska_ca))

    return agreements


def update_alaska_agreements():

    bz.quick_access_va(session)

    updates = get_updated_agreements()
    
    for update in updates:

        # voids
        if 'END DTE' in updates[update] and 'STR DTE' in updates[update]:
            print('VOID')
        elif 'END DTE' in updates[update]:
            bz.end_vendor_agreement(session, update, updates[update]['END DTE'])
            database_command(sql.update_term_dates(update, 'END DTE', updates[update]['END DTE']))


if __name__ == "__main__":

    # load new agreements keyed yesterday by DPM
    try:
        load_alaska_agreements()
    except Exception as e:
        print(f'Could not process all new agreements - check job:\n{e}')

    # update alaska agreements to align with an updated lead agreement
    try:
        update_alaska_agreements()
    except Exception as e:
        print(f'Could not process all agreement updates - check job:\n{e}')

    # close bluezone sessioon
    bz.disconnect(session)

    # send all vadam tie requests to west market field
    try:
        send_vadam_request(database_query(sql.get_vendor_errors))
    except Exception as e:
        print(f'Could not send VADAM request - check job:\n{e}')



'''
i want to change the upload_alaska_agreements() function to also update 
the timestamp and change code of updated agreements. That way, 
the query can just handle new and updated agreements accordingly
create dictionary of actions with 'NEW' and 'CHANGE' as the keys 
including subsequent dictionaries 

I have ending vendor agreements configured, but not customer agreements
and not voids
'''

