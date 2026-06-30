import os
from datetime import datetime, timedelta
from common import send_outlook_mail, root_dir, today

heartbeat_file = os.path.join(root_dir,'heartbeat.txt')
expected_runtimes = ['08:35','15:34','16:35','20:38']
to_email = ['vipulanand@rathi.com']
cc_email = ['ronakmoondra1@rathi.com', 'anirudhadurgule@rathi.com']
grace_minutes = 2
alert_file = os.path.join(root_dir,'alert_sent.txt')

current_dtt = datetime.now()
for expected_time in expected_runtimes:
    expected_dtt = datetime.strptime(f'{today} {expected_time}', '%Y-%m-%d %H:%M')
    
    monitor_time = expected_dtt + timedelta(minutes=grace_minutes)
    if current_dtt > monitor_time:
        if not os.path.exists(heartbeat_file):
            should_alert = True
        else:
            heartbeat_time = datetime.fromtimestamp(os.path.getmtime(heartbeat_file))
            should_alert = heartbeat_time < expected_dtt
        
        alert_key = f'{today}_{expected_dtt}'
        already_alerted = False
        if os.path.exists(alert_file):
            with open(alert_file, 'r') as f:
                alerted_runs = f.read().splitlines()
                if alert_key in alerted_runs:
                    already_alerted = True
        if should_alert and not already_alerted:
            subject = f"ALERT - NOTIS Scheduler NOT Triggered | {expected_time}"
            
            body = f"""
                NOTIS scheduled execution was NOT triggered.
    
                Expected Run Time   : {expected_dtt}
                Current Time        : {current_dtt}
                Heartbeat File      : {heartbeat_file}
    
                Reason              :Schedular did not ran notis app as per the scheduled time.
    
                Machine             :{os.environ.get('COMPUTERNAME')}
    
                Regards,
                NOTIS Automation
            """
            
            send_outlook_mail(
                subject=subject,
                body=body,
                to_emails=to_email,
                cc_emails=cc_email
            )
            
            with open(alert_file,'a') as f:
                f.write(alert_key + '\n')