import time
import win32com.client
import pythoncom


def bypassWarning(bzo):
    while checkScreen(bzo, 'Systems', 1) == False:
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

    
def checkScreen(bzo, screen, row):
    sys = '' 
    sys = bzo.ReadScreen(sys, 80, row, 1)[1]

    if screen in sys:
        return True
    else:
        return False


def disconnect(bzo):
    bzo.CloseSession(1,26)


def connect():
    pythoncom.CoInitialize()
    bzo = win32com.client.Dispatch("BZWhll.WhllObj")

    bzo.OpenSession(1, 26, "SUS", 30, 1)
    bzo.Connect("Z")
    bzo.SendKey("<f3><enter>")
    bzo.WaitReady(10, 0)
    bzo.DisconnectFromHost(1,0)

    time.sleep(0.3)

    host = "SUS450"
    port = "23"

    bzo.HostName = host
    bzo.TelnetPort = port
    bzo.ConnectToHost(1,0)
    bzo.waitready(10,0)

    if checkScreen(bzo, 'Systems', 1) == False: bypassWarning(bzo)

    return bzo


def quick_access_va(bzo):
    screen = "Vendor Agreement Maintenance"

    if checkScreen(bzo, screen, 1) == False:
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF10>")
        bzo.WaitReady(10,1)
        bzo.SendKey("VA")
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, screen, 1) == False:
            time.sleep(0.3)


def quick_access_ca(bzo):
    screen = "Customer Agreement Maintenance"

    if checkScreen(bzo, screen, 1) == False:
        bzo.SendKey("<PF10>")
        bzo.WaitReady(10,1)
        bzo.SendKey("CA")
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, screen, 1) == False:
            time.sleep(0.3)


def create_va(bzo, type, vendor):
    startScreen = "Vendor Agreement Maintenance"
    endScreen = "Agreement Header"
    errScreen = {
        "VA10023":"VA Default Address required for Vendor Hierarchy.",
        "VA10024":"Vendor number is invalid. Press F4 for a list."
    }


    if checkScreen(bzo, startScreen, 1) == True:
        vendor = (f'          {vendor}')[-10:]
        bzo.WriteScreen(type, 10, 45)
        bzo.WriteScreen(vendor, 12, 45)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF6>")
        bzo.WaitReady(10,1)

        va = ''
        for err in errScreen:
            if checkScreen(bzo, errScreen[err], 24) == True:
                va = err
                bzo.SendKey("<PF15>")
                bzo.WaitReady(10,1)

        if va == '':
            while checkScreen(bzo, endScreen, 2) == False:
                time.sleep(0.3)

            va = bzo.ReadScreen(va, 9, 4, 16)[1]

        return va
        

def va_general_agreement_information(bzo, description, pastDueDeduct, costBasis, caType, rebateType, start, end):
    startScreen = 'General Agreement Information'
    endScreen = 'Billing Information'
    warningScreen = 'Warning: Changes will trigger retro processing.'

    if checkScreen(bzo, startScreen, 7) == True:
        if len(description) > 30: description = description[-30:]
        pastDueDeduct = (f'    {pastDueDeduct}')[-4:]
        start = (f'        {start}')[-8:]
        end = (f'        {end}')[-8:]
        
        bzo.WriteScreen(description, 4, 28)
        bzo.WriteScreen('CORP', 8, 51)
        bzo.WriteScreen(pastDueDeduct, 10, 51)
        bzo.WriteScreen(costBasis, 14, 51)
        if caType != '': 
            bzo.WriteScreen('Y', 15, 51)
            bzo.WriteScreen(caType, 16, 51)
            bzo.WriteScreen(rebateType, 17, 51)
        bzo.WriteScreen(start, 19, 51)
        bzo.WriteScreen(end, 20, 51)

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 8) == False:
            time.sleep(0.3)
            if checkScreen(bzo, warningScreen, 24): 
                bzo.SendKey("<Enter>")
                bzo.WaitReady(10,1)


def va_billing_information(bzo, billingFreq1, billingFreq2, billBackFormat, preApproval, corporateClaimed, appropName):
    startScreen = 'Billing Information'
    warningScreen = 'Warning: Changes will trigger retro processing.'
    nextScreen = 'Agreement Address/Codes'
    endScreen = 'Item Eligibility'
    

    if checkScreen(bzo, startScreen, 8) == True:
        billingFreq2 = (f'  {billingFreq2}')[-2:]

        bzo.WriteScreen(billingFreq1, 9, 24)
        bzo.WriteScreen(billingFreq2, 9, 28)
        bzo.WriteScreen(billBackFormat, 9, 66)
        bzo.WriteScreen(preApproval, 10, 30)
        bzo.WriteScreen(corporateClaimed, 14, 27)
        bzo.WriteScreen(appropName, 20, 27)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        
        while checkScreen(bzo, nextScreen, 2) == False:
            time.sleep(0.3)
            if checkScreen(bzo, warningScreen, 24): 
                bzo.SendKey("<Enter>")
                bzo.WaitReady(10,1)

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)


def va_item_eligibility(bzo, items):
    startScreen = 'Item Eligibility'
    warningScreen = 'Item does not belong to the vendor specified on the vendor agreement.'
    endScreen = 'Customer Eligibility'

    if checkScreen(bzo, startScreen, 2) == True:

        row = 13
        for item in items:
            bzo.WriteScreen('A', row, 4)
            bzo.WriteScreen('SUPC', row, 6)
            bzo.WriteScreen(item.ITEM, row, 15)
            bzo.WriteScreen(item.VA_REBATE_BASIS, row, 36)
            bzo.WriteScreen(item.VA_ALASKA_AMT, row, 42)
            bzo.WriteScreen(item.VA_APPROP_AMT, row, 55)

            if row == 20: 
                bzo.SendKey("@v")
                bzo.WaitReady(10,1)
                time.sleep(0.3)
                row = 13 
            else:
                row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)
            if checkScreen(bzo, warningScreen, 24): 
                bzo.SendKey("<Enter>")
                bzo.WaitReady(10,1)


def va_customer_eligibility(bzo, customers):
    startScreen = 'Customer Eligibility'
    nextScreen = 'Agreement Overrides'
    endScreen = nextScreen if customers[0].CA == '0' else 'Customer Agreement Item Eligibility (Allowance)'

    if checkScreen(bzo, startScreen, 2) == True:
        
        if 'VA' in customers[0].ELIGIBILITY_TYPE:  

            row = 13
            for customer in customers:
                bzo.WriteScreen(customer.IEA, row, 8)
                bzo.WriteScreen(customer.SPEC_CODE, row, 13)
                bzo.WriteScreen(customer.SPEC, row, 19)
                bzo.WriteScreen(f'  {customer.START_DT}', row, 37)
                bzo.WriteScreen(f'  {customer.END_DT}', row, 48)

                if row == 20: 
                    bzo.SendKey("@v")
                    bzo.WaitReady(10,1)
                    time.sleep(0.3)
                    row = 13 
                else:
                    row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, nextScreen, 2) == False:
            time.sleep(0.3)

        if nextScreen != endScreen:
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)            

            while checkScreen(bzo, endScreen, 2) == False:
                time.sleep(0.3)


def ca_item_eligibility(bzo, items):
    startScreen = 'Customer Agreement Item Eligibility (Allowance)'
    endScreen = 'Customer Eligibility'

    if checkScreen(bzo, startScreen, 2) == True:

        row = 12
        for item in items:
            bzo.WriteScreen(item.CA_REBATE_BASIS, row, 33)
            bzo.WriteScreen('         ', row, 36)
            bzo.WriteScreen(item.CA_ALLOWANCE, row, 36)
            bzo.WriteScreen('        ', row, 48)
            bzo.WriteScreen(item.CA_COMM_BASE, row, 48)
            bzo.WriteScreen('        ', row, 57)
            bzo.WriteScreen(item.CA_ALASKA_ADJ_AP, row, 57)
            
            if row == 20: 
                bzo.SendKey("@v")
                bzo.WaitReady(10,1)
                time.sleep(0.3)
                row = 13 
            else:
                row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)


def ca_customer_eligibility(bzo, customers):
    startScreen = 'Customer Eligibility'
    endScreen = 'Agreement Overrides'
    ca = ''

    if checkScreen(bzo, startScreen, 2) == True:

        if 'VA' not in customers[0].ELIGIBILITY_TYPE:  
            row = 13
            for customer in customers:
                bzo.WriteScreen(customer.IEA, row, 9)
                bzo.WriteScreen(customer.SPEC_CODE, row, 15)
                bzo.WriteScreen(customer.SPEC, row, 22)
                bzo.WriteScreen(f'  {customer.START_DT}', row, 40)
                bzo.WriteScreen(f'  {customer.END_DT}', row, 50)

                if row == 20: 
                    bzo.SendKey("@v")
                    bzo.WaitReady(10,1)
                    time.sleep(0.3)
                    row = 13  
                else:
                    row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)

        ca = bzo.ReadScreen(ca, 9, 4, 15)[1]
        
    if not ca.isnumeric(): ca = '0'
    
    return ca
    

def commit_transaction(bzo):
    startScreen = 'Agreement Overrides'
    endScreen = 'Agreement Maintenance'
    
    if checkScreen(bzo, startScreen, 2) == True:
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF7>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, endScreen, 1) == False:
            time.sleep(0.3)

