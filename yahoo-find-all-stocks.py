import urllib2
import sys
import time
import sqlite3

def get_page(page):
    print('getting data from page %s'%page)
    data = urllib2.urlopen(page).read()
    row = data.find('<tr><td\nnowrap')
    out = []
    while row >= 0:
        tmp = data[row:]
        url = tmp[tmp.find('href=')+5:tmp.find('<font\nface')-1]
        x = tmp.find('size=-1>')+8
        category = tmp[x:tmp.find('</a>',x)].replace('\n','')
        row = data.find('<tr><td\nnowrap', x+row)
#        print '<a href="%s">%s</a>'%(url, category)
        newx_s = 'http://us.rd.yahoo.com/finance/industry/quote/colist/*http://biz.yahoo.com/p/'
        newx = category.find(newx_s)
        if url != '':
            out.append( (url, category) )
        elif newx >= 0:
            begin = newx+len(newx_s)
            rbegin = category.find('/',begin)
            if rbegin >= 0:
                rend = category.find('.html">',begin)
                ticker = category[rbegin+1:rend]
                name   = category[rend+7:category.find('</a>',rend+7)]
                out.append( (ticker.upper(), name) )
    time.sleep(0.2)
    return out

stocks_conn = sqlite3.connect("stocks.db")
stocks_cur  = stocks_conn.cursor()
stocks_cur.execute('CREATE TABLE IF NOT EXISTS stocks (sector text, industry text, ticker text, inserted text);')
stocks_cur.execute('CREATE INDEX IF NOT EXISTS idx_sector_industry ON stocks(sector,industry)')
stocks_cur.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON stocks(ticker)')

root = 'http://biz.yahoo.com/p'
top_level = '%s/s_conameu.html'%root
sectors = get_page(top_level)
for i,sector in enumerate(sectors):
    url_sec, sector_name = sector
    industries = get_page('%s/%s'%(root,url_sec))
    for industry in industries:
        url_ind, industry_name = industry[0], industry[1].replace('</font>','')
        stocks = get_page('%s/%s'%(root,url_ind))
        print(sector_name,industry_name,len(stocks),stocks[0])
        for stock in stocks:
            ticker,stock_name = stock
            stocks_cur.execute('INSERT INTO stocks SELECT ?,?,?,? WHERE NOT EXISTS (SELECT ticker FROM stocks WHERE ticker=?)',
                                (str(sector_name),str(industry_name),str(ticker),str(time.time()),str(ticker)))
            stocks_conn.commit()
