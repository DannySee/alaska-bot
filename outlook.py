import win32com.client
from datetime import date

today = date.today()
today = today.strftime("%m.%d.%Y")

outlook = win32com.client.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.To = 'daniel.clark@sysco.com'
mail.Subject = f'Alaska VADAM Tie Request'
mail.Body = f"""
    Good afternoon,
    
    In order to key Alaska deviations, VADAM ties are required for the attached (and below) vendors in as450a. Please advise if any additional information is needed.

    {vendors}

    Thanks, 
    SBS Pricing & Agreements Quality Assurance
"""
#mail.SentOnBehalfOfName = 'qapricingagreements@sbs.sysco.com'


def send_mail(attachment):
    mail.Attachments.Add(attachment)
    mail.Send()
