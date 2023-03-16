import smtplib
from email.mime.text import MIMEText


def send_alert_msg(pid, msgs):
    # creates SMTP session
    msg = MIMEText('id = {} {}' .format(pid, msgs))
    s = smtplib.SMTP('smtp.office365.com', 587)
    # start TLS for security
    s.starttls()
    sender = "revseed@revnomix.com"
    email_id = 'monitoring-team@revnomix.com'
    recipients = email_id
    msg['Subject'] = 'id = {} :'.format(pid) + ' ' + msgs
    msg['From'] = sender
    msg['To'] = ' ,'.join([str(elem) for elem in recipients.split(',')[:1]])
    msg['Cc'] = ' ,'.join([str(elem) for elem in recipients.split(',')[1:]])
    # Authentication
    s.login("revseed@revnomix.com", "@#Rev2022@321~")
    s.sendmail(sender, recipients.split(','), msg.as_string())
    # s.sendmail(sender,  recipients.split(','), msg.as_string())
    s.quit()





