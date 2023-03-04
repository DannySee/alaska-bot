import time
import win32com.client
import pythoncom


def bypassWarning(bzo):
    while checkScreen(bzo, 'Systems', 1) == False:
        bzo.SendKey("<Enter>")

    
def checkScreen(bzo, screen, row):
    sys = '' 
    sys = bzo.ReadScreen(sys, 80, row, 1)[1]

    if screen in sys:
        return True
    else:
        return False


def disconnect(bzo):
    bzo.CloseSession(1,1)


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
        bzo.SendKey("<PF10>")
        bzo.WaitReady(10,1)
        bzo.SendKey("VA")
        bzo.SendKey("<Enter>")
        bzo.WaitReady(10,1)

        while checkScreen(bzo, screen, 1) == False:
            time.sleep(0.3)


def create_va(bzo, type, vendor):
    mainScreen = "Vendor Agreement Maintenance"
    subScreen = "Agreement Header"

    if checkScreen(bzo, mainScreen, 1) == True:
        vendor = (f'          {vendor}')[-10:]
        bzo.WriteScreen(type, 10, 45)
        bzo.WriteScreen(vendor, 12, 45)
        bzo.SendKey("<Enter>")
        bzo.SendKey("<PF6>")

        while checkScreen(bzo, subScreen, 2) == False:
            time.sleep(0.3)


def va_general_agreement_information(bzo, description, pastDueDeduct, costBasis, caType, rebateType, start, end):
    startScreen = 'General Agreement Information'
    endScreen = 'Billing Information'
    retroWarning = 'Warning: Changes will trigger retro processing.'

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

        while checkScreen(bzo, endScreen, 8) == False:
            time.sleep(0.3)
            if checkScreen(bzo, retroWarning, 24): bzo.SendKey("<Enter>")


def va_billing_information(bzo, billingFreq1, billingFreq2, billBackFormat, preApproval, corporateClaimed, appropName):
    startScreen = 'Billing Information'
    nextScreen = 'Agreement Address/Codes'
    endScreen = 'Item Eligibility'
    retroWarning = 'Warning: Changes will trigger retro processing.'

    if checkScreen(bzo, startScreen, 8) == True:
        billingFreq2 = (f'  {billingFreq2}')[-2:]

        bzo.WriteScreen(billingFreq1, 9, 24)
        bzo.WriteScreen(billingFreq2, 9, 28)
        bzo.WriteScreen(billBackFormat, 9, 66)
        bzo.WriteScreen(preApproval, 10, 30)
        bzo.WriteScreen(corporateClaimed, 14, 27)
        bzo.WriteScreen(appropName, 20, 27)
        bzo.SendKey("<Enter>")
        
        while checkScreen(bzo, nextScreen, 2) == False:
            time.sleep(0.3)
            if checkScreen(bzo, retroWarning, 24): bzo.SendKey("<Enter>")

        bzo.SendKey("<Enter>")

        while checkScreen(bzo, endScreen, 2) == False:
            time.sleep(0.3)


def va_item_eligibility(bzo, iea, specCode, spec, rebateBasis, rebateAmount, approprAmount, start, end):
    print('Doh')