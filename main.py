import session_manager as bz

from data_pull import upload_alaska_deviations, deviation_details, new_agreement
from outlook import send_vadam_request


# load list of alaska deviations 
#deviations = upload_alaska_deviations()
deviations =  deviation_details()
header = deviations['header']
itemEligibility = deviations['item_eligibility']
customerEligibility = deviations['customer_eligibility']

# open an blank bluezone session
session = bz.connect()

# loop through header list - one iteration per agreement
for agmt in header:

    # set agreement number variables to 0
    va = '0'
    ca = '0'

    # handle vendor and customer agreements differently
    if agmt.VA != '0':

        # go to quick access va
        bz.quick_access_va(session)

        # create new agreement
        va = bz.create_va(session, agmt.VA_TYPE, agmt.VENDOR_NBR)

        # key geenral agreement informatoin screen
        bz.va_general_agreement_information(session, agmt.DESCRIPTION, agmt.PAST_DUE_DEDUCT, agmt.ALASKA_COST_BASIS, agmt.CA_TYPE, agmt.REBATE_TYPE, agmt.START_DT, agmt.END_DT)

        # key billing information screen
        bz.va_billing_information(session, agmt.BILLING_FREQ, agmt.BILLING_DAY, agmt.BILLBACK_FORMAT, agmt.PRE_APPROVAL, agmt.CORP_CLAIMED, agmt.APPROP_NAME)

        # key va item eligibility
        bz.va_item_eligibility(session, itemEligibility[agmt.VA + agmt.CA])

        # key customer eligibility if billbacks are allowed
        bz.va_customer_eligibility(session, customerEligibility[agmt.VA + agmt.CA])

        # continue through agreement creation if there is a customer side of the agreement then 
        if agmt.CA_TYPE != '':
            
            # key ca item eligibility
            bz.ca_item_eligibility(session, itemEligibility[agmt.VA + agmt.CA])

            # key ca customer eligibility return ca number
            ca = bz.ca_customer_eligibility(session, customerEligibility[agmt.VA + agmt.CA])
    else:
        
        # go to quick access ca
        bz.quick_access_ca(session)

        # create new agreement
        ca = bz.create_va(session, agmt.CA_TYPE)

    # update (F7) agreement and save to server
    bz.commit_transaction(session)
    new_agreement(agmt.PRIMARY_KEY, va, ca)

    print(f'va:{va}\nca:{ca}')

# close bluezone sessioon
bz.disconnect(session)

# send all vadam tie requests to west market field
send_vadam_request()


    

    
        