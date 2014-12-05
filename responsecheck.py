import configparser
from subprocess import PIPE, Popen
import sys

import pymysql


__author__ = 'hanzki'

config = configparser.ConfigParser()


def save_data(conn, loc, s3, ip, nlag, rlag):
    cur = conn.cursor()
    sql = 'INSERT INTO cndb.RESPONSE_TIMES ' \
          '(TIME, MYLOC, S3LOC, IP, NETWORK_LAG, RETRIEVAL_LAG) ' \
          'VALUES ' \
          '(NOW(), "%s", "%s", "%s", %d, %d)' % \
          (loc, s3, ip, nlag, rlag)
    print(sql)
    cur.execute(sql)
    for response in cur:
        print(response)
    cur.close()


def get_ip_nlag_rlag(url):
    p1 = Popen(['curl',
                '-o /dev/null',
                '-s',
                '-w %{remote_ip}##%{time_connect}##%{time_starttransfer}',
                url], stdout=PIPE)
    result = p1.communicate()[0].decode()
    parts = result.replace(',', '.').split("##")
    ip = parts[0].strip()
    nlag = round(float(parts[1]) * 1000)
    rlag = round((float(parts[2]) - float(parts[1])) * 1000)

    return ip, nlag, rlag


def main(argv):
    config.read(argv[0])
    conn = pymysql.connect(user=config.get('mysqldb', 'user'),
                           passwd=config.get('mysqldb', 'passwd'),
                           host=config.get('mysqldb', 'host'),
                           db=config.get('mysqldb', 'db'),
                           autocommit=True)

    loc = config['general']['location']

    for server in config['servers']['locations'].split():
        for file in config['servers']['files'].split():
            url = 'http://%s/%s' % (config[server]['domain'], file)
            ip, nlag, rlag = get_ip_nlag_rlag(url)
            save_data(conn, loc, config[server]['name'], ip, nlag, rlag)

    conn.close()

if __name__ == "__main__": main(sys.argv[1:])
