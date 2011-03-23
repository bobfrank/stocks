import re

def tryfloat(x):
    try:
        return float(x.replace(',',''))
    except:
        return x

def extract_table(table_str):
    rsts=[]
    row_re=re.compile("<tr[^<>]*>.*?</tr>",re.S|re.I)
    col_re= re.compile(r"<(td|th)[^<>]*>(.*?)</\1>",re.S|re.I)
    tag_re =re.compile("<[^<>]+>",re.S|re.I)
    blank_re=re.compile('\s+',re.S|re.I)
    rows=row_re.findall(table_str)
    for row_str in rows:
        cols=col_re.findall(row_str)
        cols=[tag_re.sub('',col_str[1]) for col_str in cols]
        cols=[blank_re.sub('',col_str) for col_str in cols]
        rsts.append(cols)
    return rsts

def transposed(lists):
   if not lists: return []
   return map(lambda *row: list(row), *lists)

import urllib2

global data
#data = urllib2.urlopen('http://www.google.com/finance?q=NASDAQ:GOOG&fstype=ii&authuser=0').read()
k = data.find('table')
while k >= 0:
    j = data.find('</table>',k+1)
    table = transposed(extract_table(data[k:j]))
    print [[tryfloat(cell) for cell in row] for row in table]
    k = data.find('table',j+5)
