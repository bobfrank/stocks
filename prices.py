import urllib2
import sqlite3
import datetime
import csv
import StringIO
import traceback
import time

stocks_conn = sqlite3.connect('stocks.db')
stocks_cur  = stocks_conn.cursor()

years = range(2011,1979,-1)
for YEAR in years:
    prices_conn = sqlite3.connect('prices_%s.db'%YEAR)
    prices_cur  = prices_conn.cursor()

    prices_cur.execute("CREATE TABLE IF NOT EXISTS prices (ticker text, date text, open real, high real, low real, close real, volume real, adj_close real)")
    prices_cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker_date ON prices(ticker,date)")
    prices_cur.execute("CREATE INDEX IF NOT EXISTS idx_date ON prices(date)")

    end_date = datetime.datetime.now()
    if end_date.year > YEAR:
        end_date = datetime.datetime(YEAR,12,31)

    for row in stocks_cur.execute("SELECT ticker FROM stocks"):
        ticker = row[0]
        rows = list(prices_cur.execute("SELECT max(date) FROM prices WHERE ticker=?", (ticker,)))
        if len(rows) > 0 and len(rows[0]) > 0 and rows[0][0] is not None:
            prev_date = [int(x) for x in rows[0][0].split('-')] #so input is like 2011-01-01
        else:
            prev_date = [YEAR,1,1]
        start_date = datetime.datetime(*tuple(prev_date))+datetime.timedelta(1)
        print(ticker,start_date,end_date)
        if start_date >= end_date:
            print('already updated, skipping')
            continue
        #continue
        retry = True
        newdata = None
        while retry:
            try:
                nfp=urllib2.urlopen('http://ichart.finance.yahoo.com/table.csv?s=%s&a=%d&b=%d&c=%d&d=%d&e=%d&f=%d&g=d&ignore=.csv'
                    % (ticker, start_date.month-1, start_date.day, start_date.year,
                            end_date.month-1, end_date.day, end_date.year))
                newdata=nfp.read()
                retry = False
            except KeyboardInterrupt:
                raise
            except urllib2.HTTPError, e:
                if e.code == 404:
                    newdata=None
                    break
                else:
                    continue
            except:
                traceback.print_exc()
                time.sleep(1)
                continue
        if newdata is None:
            continue
        data_io = StringIO.StringIO(newdata)
        csv_reader = csv.reader(data_io)
        rows = list(csv_reader)[1:]
        for row in rows:
            prices_cur.execute("INSERT INTO prices SELECT '%s',?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT ticker FROM prices WHERE ticker=? AND date=?)"%ticker,
                                                    (row[0],row[1],row[2],row[3],row[4],row[5],row[6],ticker,row[0]))
        prices_conn.commit()
    prices_conn.close()
