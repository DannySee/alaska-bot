import os

from data_centers import sql_server
from data_pull import vadam_ties
from excel import create_excel


def send_vadam_request():

    vendors = vadam_ties()
    if len(vendors) > 0:
        vendors = [int(vendor[0]) for vendor in vendors]
        vendorList = ",\n".join(str(vendor) for vendor in vendors)

        to = 'westmarket_profitmanagement@sysco.com;daniel.clark@sysco.com'
        subject = f'Alaska VADAM Tie Request'
        body = (f"Good afternoon,\n\nIn order to key Alaska deviations, VADAM ties are required for the attached (and below) vendors in as450a. Please advise if any additional information is needed.\n\n{vendorList}\n\nThanks,\nSBS Pricing & Agreements Quality Assurance")
        attachment = create_excel(vendors)

        # Execute the stored procedure to send the email
        sql_server.execute("""
        DECLARE @MailProfile VARCHAR(50) = 'Scheduled_Reporting';
        DECLARE @Subject NVARCHAR(255) = ?;
        DECLARE @Body NVARCHAR(MAX) = ?;
        DECLARE @Recipients NVARCHAR(MAX) = ?;
        DECLARE @Attachment NVARCHAR(MAX) = ?;

        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = @MailProfile,
            @recipients = @Recipients,
            @blind_copy_recipients = 'daniel.clark@sysco.com',
            @subject = @Subject,
            @body = @Body,
            @file_attachments = @Attachment;
        """, subject, body, to, attachment)

        # Commit the transaction and close the connection
        sql_server.commit()
        sql_server.close()

        # Erase file
        os.remove(attachment)

