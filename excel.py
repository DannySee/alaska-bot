import os
import pandas as pd


def create_excel(vendors):
    filename = 'C:\\Temp\\outgoing_mail\\Alaska VADAM Ties.xlsx'
    
    with pd.ExcelWriter(filename) as writer:
        df = pd.DataFrame(vendors, columns=['vendors'])
        df.to_excel(writer, sheet_name='VADAM Requests', index=False)

    return filename
