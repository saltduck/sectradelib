# coding:utf8
import logging
import os
import threading

logger = logging.getLogger(__name__)

def check_running(PIDFILE):
    if os.path.exists(PIDFILE):
        with open(PIDFILE, 'r') as f:
            try:
                s = f.readline()
                pid = int(s)
            except IOError:
                print 'Cannot read ' + PIDFILE + '!'
                is_running = True
            except ValueError:
                print PIDFILE + ' content is invalid'
                is_running = True
            else:
                try:
                    os.kill(pid, 0)
                except OSError:
                    is_running = False
                else:
                    print 'This program is running, DONOT run it again!'
                    is_running = True
    else:
        is_running = False
    if not is_running:
        with open(PIDFILE, 'w') as f:
            f.write(str(os.getpid()))
    return is_running

def align_digit(volume, tick_size):
    """
    >>> align_digit(3, 1.0)
    3.0
    >>> align_digit(3.0, 1.0)
    3.0
    >>> align_digit(3.7234, 0.25)
    3.5
    >>> align_digit(3.7834, 0.25)
    3.75
    """
    return int(volume / tick_size) * tick_size

def send_account_email(interval, app, server, port, username, password, sendtolist):
    logger.info(u'发送资金变动通知email...')
    import smtplib
    from email.mime.text import MIMEText
    try:
        smtp = smtplib.SMTP_SSL()
        smtp.connect(server, port)
        smtp.login(username, password)
        fromaddr = username
        toaddrs = sendtolist
        text = u'账户资金变动通知：\n'
        text += '\n'.join([u'账号：%s    可用资金：%.2f    保证金：%.2f' % (api.accountcode,  api.available, api.margins) for api in app.traderapis])
        msg = MIMEText(text, 'plain', 'utf8')
        msg['From'] = fromaddr
        msg['To'] = ','.join(toaddrs)
        msg['Subject'] = 'CTP账户余额'
        smtp.sendmail(fromaddr, toaddrs, msg.as_string())
        smtp.quit()
    except smtplib.SMTPException, e:
        logger.exception(unicode(e))
    t = threading.Timer(
            interval,
            send_account_email,
            (interval, app, server, port, username, password, sendtolist)
    )
    app.threads.append(t)
    t.start()
