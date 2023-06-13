from excel import create_excel

import win32com.client

# create outlook object
outlook = win32com.client.Dispatch('outlook.application')
mail = outlook.CreateItem(0)


# send vadam tie request to west market based on agreements failed due to vendor errors
def send_vadam_request(vendors):

    if len(vendors) > 0:
        vendors = [int(vendor[0]) for vendor in vendors]
        vendorList = ",\n".join(str(vendor) for vendor in vendors)

        mail.To = 'westmarket_profitmanagement@sysco.com'
        mail.Subject = f'Alaska VADAM Tie Request'
        mail.Body = (f"Good afternoon,\n\nIn order to key Alaska deviations, VADAM ties are required for the attached (and below) vendors in as450a. Please advise if any additional information is needed.\n\n{vendorList}\n\nThanks,\nSBS Pricing & Agreements Quality Assurance")

        attachment = create_excel(vendors)
        mail.Attachments.Add(attachment)
        mail.Send()


