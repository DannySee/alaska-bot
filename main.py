import session_manager as bz
import data_pull as query


# load list of alaska deviations
query.upload_alaska_deviations



session = bz.connect()
bz.quick_access_va(session)
bz.create_va(session, 'CNVA', '1')
bz.va_general_agreement_information(session, 'test', '999', 'D', 'CNCA', 'G', '010122', '010124')
bz.va_billing_information(session, 'M', '31', 'E', 'testing', 'Y', 'DPMEI01')