import time
import win32com.client
import pythoncom

# enter key is pressed until system warning screen is bypassed
def bypassWarning(bzo):
    while checkScreen(bzo, 'Systems', 1) == False:
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)


# read screen values form <row> coordinate and compare it to <screen> value: true = match, false = mismatch. 
def checkScreen(bzo, screen, row):
    sys = '' 
    sys = bzo.ReadScreen(sys, 80, row, 1)[1]

    if screen in sys:
        return True
    else:
        return False


# disconnect and end bluezone sessoin
def disconnect(bzo):
    bzo.CloseSession(1,26)


# create bluezone session and return object
def connect():
    pythoncom.CoInitialize()
    bzo = win32com.client.Dispatch("BZWhll.WhllObj")

    # define session properties and open bluezone window
    bzo.OpenSession(1, 26, "SUS", 30, 1)
    bzo.Connect("Z")
    bzo.SendKey("<f3><enter>")
    bzo.WaitReady(10, 0)
    bzo.DisconnectFromHost(1,0)

    # wait time is necessary for consistent operation (sus is slow)
    time.sleep(0.3)

    # connect to alaska
    host = "SUS450"
    port = "23"
    bzo.HostName = host
    bzo.TelnetPort = port
    bzo.ConnectToHost(1,0)
    bzo.waitready(10,0)

    # bypass any warning screens that appear
    if checkScreen(bzo, 'Systems', 1) == False: bypassWarning(bzo)

    return bzo


# navigate to quick access va
def quick_access_va(bzo):

    # define read screen checkpoints
    ca_screen = 'Customer Agreement Maintenance'
    end_screen = "Vendor Agreement Maintenance"

    # sus cannot quick access to vendor maintenance directly from the ca screen
    # exit to main screen if current screen is ca
    if checkScreen(bzo, ca_screen, 1) == True:
        bzo.SendKey("<PF3>")
        bzo.WaitReady(10,1)

    # enter quick access va and wait for screen to load
    if checkScreen(bzo, end_screen, 1) == False:
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF10>")
        bzo.WaitReady(10,1)
        bzo.SendKey("VA")
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, end_screen, 1) == False:
            time.sleep(0.3)


# navigate to quick access ca
def quick_access_ca(bzo):

    # define read screen checkpoint
    screen = "Customer Agreement Maintenance"

    # enter quick access va and wait for screen to load
    if checkScreen(bzo, screen, 1) == False:
        bzo.SendKey("<PF10>")
        bzo.WaitReady(10,1)
        bzo.SendKey("CA")
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, screen, 1) == False:
            time.sleep(0.3)


# create new vendor agreement and return va number
def create_va(bzo, type, vendor):

    # define read screen checkpoints
    startScreen = "Vendor Agreement Maintenance"
    endScreen = "Agreement Header"
    errScreen = {
        "VA10023":"VA Default Address required for Vendor Hierarchy.",
        "VA10024":"Vendor number is invalid. Press F4 for a list."
    }

    # enter vendor number/greement type and f6 to create new va
    if checkScreen(bzo, startScreen, 1) == True:
        vendor = (f'          {vendor}')[-10:]
        bzo.WriteScreen(type, 10, 45)
        bzo.WriteScreen(vendor, 12, 45)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF6>")
        bzo.WaitReady(10,1)

        # get va number for return. return error message for invalid vendor
        va = ''
        for err in errScreen:
            if checkScreen(bzo, errScreen[err], 24) == True:
                va = err
                bzo.SendKey("<PF15>")
                bzo.WaitReady(10,1)

        if va == '':

            # wait for screen to load
            while checkScreen(bzo, endScreen, 2) == False:
                time.sleep(0.3)

            va = bzo.ReadScreen(va, 9, 4, 16)[1]

        return va


# create new customer agreement and return ca number
def create_ca(bzo, type, rebate_type):

    # define read screen checkpoints
    startScreen = "Customer Agreement Maintenance"
    endScreen = "Customer Agreement Header (Allowance)"

    ca = ''

    # enter agreement/rebate type and f6 to create new ca
    if checkScreen(bzo, startScreen, 1) == True:
        bzo.WriteScreen(type, 11, 48)
        bzo.WriteScreen(rebate_type, 13, 48)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF6>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)

        ca = bzo.ReadScreen(ca, 9, 4, 21)[1]

        return ca


# navigate to agreement maintenance for <va> 
def maintain_va(bzo, va):

    # define read screen checkpoints
    startScreen = "Vendor Agreement Maintenance"
    endScreen = "Agreement Header"
    errScreen = "Vendor Agreement Number is invalid. Press F4 for a list."

    # enter existing va number and enter through for maintenance 
    if checkScreen(bzo, startScreen, 1) == True:
        bzo.WriteScreen(va, 8, 45)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        if checkScreen(bzo, errScreen, 24) == True: return errScreen

        # wait for screen to load
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3) 

        return 'success'


# update vendor agreement <va> with new <end_date> value
def end_vendor_agreement(bzo, va, end_date):

    # define read screen checkpoints
    startScreen = "Agreement Header"
    endScreen = "Press F7 to Update, F3 to Exit, or F15 to return to prompt."
    errScreen = "Invalid date; must be in MMDDYY format."

    # go into va maintenance and end function if error
    ret_val = maintain_va(bzo, va)
    if ret_val != 'success': return ret_val

    # update agreeement end date if active screen is correct
    if checkScreen(bzo, startScreen, 2) == True:
        end_date = (f'        {end_date}')[-8:]

        bzo.WriteScreen(end_date, 20, 51)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        if checkScreen(bzo, errScreen, 24) == True: 
            bzo.SendKey("<PF15>")
            bzo.WaitReady(10,1)

            return errScreen

        # wait for screen to load
        while checkScreen(bzo, endScreen, 24) == False:
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)

        # f7 (update) agreement
        commit_transaction(bzo)

        return 'success'
    

# incomplete
def retro_void_agreement(bzo, va, date):
    startScreen = "Agreement Header"
    endScreen = "Press F7 to Update, F3 to Exit, or F15 to return to prompt."
    errScreen = "Invalid date; must be in MMDDYY format."

    ret_val = maintain_va(bzo, va)

    if ret_val != 'success': return ret_val

    if checkScreen(bzo, startScreen, 1) == True:
        end_date = (f'        {end_date}')[-8:]

        bzo.WriteScreen('RVOID', 4, 28)
        bzo.WriteScreen(date, 19, 51)
        bzo.WriteScreen(date, 20, 51)
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        if checkScreen(bzo, errScreen, 24) == True: 
            bzo.SendKey("<PF15>")
            bzo.WaitReady(10,1)

            return errScreen

        while checkScreen(bzo, endScreen, 24) == False:
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)

        commit_transaction(bzo)

        return 'success'


# key vendor agreement header information and enter through to va billing information screen
def va_general_agreement_information(bzo, description, pastDueDeduct, costBasis, caType, rebateType, start, end):

    # define read screen checkpoints
    startScreen = 'General Agreement Information'
    endScreen = 'Billing Information'
    warningScreen = 'Warning: Changes will trigger retro processing.'

    # enter agreement header information if active screen is correct
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

        # wait for screen to load/bypass any warnings
        while checkScreen(bzo, endScreen, 8) == False:
            time.sleep(0.3)
            if checkScreen(bzo, warningScreen, 24): 
                bzo.SendKey("<Enter>")
                bzo.WaitReady(10,1)


# key customer agreement header information and enter through to ca item eligibility screen
def ca_general_agreement_information(bzo, description, billing_freq, billing_day, approp_name, start, end):

    # define read screen checkpoints
    startScreen = 'General Agreement Information'
    endScreen = 'Customer Agreement Item Eligibility (Allowance)'

    # enter agreement header information if active screen is correct
    if checkScreen(bzo, startScreen, 8) == True:
        if len(description) > 30: description = description[-30:]
        start = (f'        {start}')[-8:]
        end = (f'        {end}')[-8:]
        billing_day = (f'  {billing_day}')[-2:]
        
        bzo.WriteScreen(description, 4, 32)
        bzo.WriteScreen('CORP', 9, 25)
        bzo.WriteScreen(billing_freq, 10, 25)
        bzo.WriteScreen(billing_day, 10, 29)
        bzo.WriteScreen(approp_name, 14, 25)
        bzo.WriteScreen(start, 17, 28)
        bzo.WriteScreen(end, 18, 28)

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load/bypass any warnings
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)
          

# key vendor agreement billing information and enter through to va item eligibility screen
def va_billing_information(bzo, billing_freq, billing_day, billBackFormat, preApproval, corporateClaimed, appropName):

    # define read screen checkpoints
    startScreen = 'Billing Information'
    warningScreen = 'Warning: Changes will trigger retro processing.'
    nextScreen = 'Agreement Address/Codes'
    endScreen = 'Item Eligibility'

    # enter agreement billing information if active screen is correct
    if checkScreen(bzo, startScreen, 8) == True:
        billing_day = (f'  {billing_day}')[-2:]

        bzo.WriteScreen(billing_freq, 9, 24)
        bzo.WriteScreen(billing_day, 9, 28)
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

        # wait for screen to load
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)


# key vendor agreement item eligibility and enter through to va customer eligibility screen
def va_item_eligibility(bzo, items):

    # define read screen checkpoints
    startScreen = 'Item Eligibility'
    warningScreen = 'Item does not belong to the vendor specified on the vendor agreement.'
    endScreen = 'Customer Eligibility'

    # enter vendor agreement item eligibliity if active screen is correct
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
                row = 13 
            else:
                row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load/bypass any warnings
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)
            if checkScreen(bzo, warningScreen, 24): 
                bzo.SendKey("<Enter>")
                bzo.WaitReady(10,1)


# key vendor agreement customer eligibility and enter through to va override screen
def va_customer_eligibility(bzo, customers):

    # define read screen checkpoints
    startScreen = 'Customer Eligibility'
    nextScreen = 'Agreement Overrides'
    endScreen = nextScreen if customers[0].CA == '0' else 'Customer Agreement Item Eligibility (Allowance)'

    # enter vendor agreement customer eligibliity if active screen is correct
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
                    row = 13 
                else:
                    row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, nextScreen, 2) == False:
            time.sleep(0.3)

        # number of pages to next screen can vary. enter through until end point is reached
        if nextScreen != endScreen:
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)
            bzo.SendKey("<Enter>")
            bzo.WaitReady(10,1)            

            # wait for screen to load
            while checkScreen(bzo, endScreen, 2) == False:
                time.sleep(0.3)


# key customer agreement item eligibility and enter through to ca customer eligibility screen
def ca_item_eligibility(bzo, items):
    
    # define read screen checkpoints
    startScreen = 'Customer Agreement Item Eligibility (Allowance)'
    endScreen = 'Customer Eligibility'

    # enter customer agreement item eligibliity if active screen is correct
    if checkScreen(bzo, startScreen, 2) == True:

        row = 12
        for item in items:
            bzo.WriteScreen('A', row, 6)
            bzo.WriteScreen('SUPC', row, 8)
            bzo.WriteScreen(item.ITEM, row, 13)
            bzo.WriteScreen(item.CA_REBATE_BASIS, row, 33)
            bzo.WriteScreen('         ', row, 36)
            bzo.WriteScreen(item.CA_ALLOWANCE, row, 36)
            bzo.WriteScreen('        ', row, 48)
            bzo.WriteScreen(item.CA_COMM_BASE, row, 48)
            bzo.WriteScreen('        ', row, 57)
            bzo.WriteScreen(item.CA_ALASKA_ADJ_AP, row, 57)
            
            if row == 19: 
                bzo.SendKey("@v")
                bzo.WaitReady(10,1)
                row = 12
            else:
                row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)


# key customer agreement customer eligibility and enter through to ca override screen
def ca_customer_eligibility(bzo, customers):

    # define read screen checkpoints
    startScreen = 'Customer Eligibility'
    endScreen = 'Agreement Overrides'
    ca = ''

    # enter customer agreement customer eligibliity if active screen is correct
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
                    row = 13  
                else:
                    row += 1 

        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)

        ca = bzo.ReadScreen(ca, 9, 4, 15)[1]
        
    if not ca.isnumeric(): ca = '0'
    
    return ca
    

# press f7 and validate update
def commit_transaction(bzo):

    # define read screen checkpoints
    startScreen = 'Agreement Overrides'
    endScreen = 'Agreement Maintenance'
    
    if checkScreen(bzo, startScreen, 2) == True:
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)
        bzo.SendKey("<PF7>")
        bzo.WaitReady(10,1)

        # wait for screen to load
        while checkScreen(bzo, endScreen, 1) == False:
            time.sleep(0.3)

