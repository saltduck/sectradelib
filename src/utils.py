# coding:utf8
import logging
import os
import threading
import time
import datetime

from decorator import decorator
import redisco
from redisco.containers import Hash

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


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


class CheckCtpMDThread(threading.Thread):

    def __init__(self, app, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.app = app
        self.name = 'CheckCtpMD'
        self.evt_stop = threading.Event()
        self.last_value = 0

    def cancel(self):
        self.evt_stop.set()

    def run(self):
        key = self.get_heartbeatkey()
        while not self.evt_stop.wait(5):
            v = int(rdb.get(key))
            if v <= self.last_value:
                logger.error(u'行情进程停止运行，尝试重启...')
                self.restart_md_process()
                # wait 5 seconds then check again
                time.sleep(5)
                key = self.get_heartbeatkey()
                v = int(rdb.get(key))
                if v is None:
                    logger.error(u'行情进程无法重启！系统停止运行')
                    self.app.on_emergency()
                    continue
                else:
                    logger.info(u'行情进程重启成功')
            self.last_value = v


@decorator
def logerror(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception, e:
        logger.exception(unicode(e))
        return


def current_price(instrumentid, direction=None):
    if direction is None:
        price = rdb.hget('current_price', instrumentid)
    elif direction:
        price = rdb.hget('current_b_price', instrumentid)
    else:
        price = rdb.hget('current_s_price', instrumentid)
    try:
        return float(price)
    except TypeError:
        logger.error(u'current_price({0}) got {1}'.format(instrumentid, price))
        return None

def last_close_price(instid):
    try:
        price = rdb.hget('last_close_price', instid)
        return price
    except TypeError:
        logger.error(u'last_close_price({0}) got {1}'.format(instid, price))
        return None

def exchange_time(exchangeid, localtime=None):
    """ 计算交易所时间 """
    if not localtime:
        localtime = datetime.datetime.now()
    timedelta = Hash('exchangetimedelta')[exchangeid]
    if timedelta:
        localtime += datetime.timedelta(seconds=float(timedelta))
    return localtime

def get_last_line(fn):
    if not os.path.exists(fn):
        return None
    with open(fn, 'r') as f:
        i = -1
        while True:
            i = i - 1
            try:
                f.seek(i, 2)
            except IOError:
                f.seek(0)
                break
            if f.read(1) == '\n':
                break
        return f.readline().strip()
