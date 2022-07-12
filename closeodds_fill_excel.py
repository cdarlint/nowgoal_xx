from pathlib import Path
import json
from pprint import pprint as print
import xlwings as xw
from nowgoal_inplay.actions import getOpenCloseOdds, Session, match
path_prefix = Path('mock_data') / 'odds_open_close'
def get_close_odds(mid,debug=False):
    # try:
    #     f = open(path_prefix / f'{mid}.txt', encoding='utf8')
    #     data=json.loads(f.read())['Data']
    #     f.close()
    # except FileNotFoundError:
    data=json.loads(getOpenCloseOdds((mid)))['Data']

    if debug:
        print(data)
    if data=='':
        return None
    datap={int(bm.split('#')[0].split(',')[1]):
           row[12:15]+row[24:27]
           for bm in data.split('!')
           for row in [(list(filter(lambda x:x.startswith('Live'),
                        bm.split('#')[1].split('^')))
            or [''])[0].split(',')]}
    if debug:
        print(datap)
    row=datap[3]+['Crown'] if 3 in datap.keys() and datap[3] and ''.join(datap[3])!='' \
        else datap[8]+['Bet365'] if 8 in datap.keys() and datap[8] and ''.join(datap[8])!='' \
        else datap[31]+['Sbobet'] if 31 in datap.keys() and datap[31] and ''.join(datap[31])!='' \
        else None
    return row

def refresh_sheet_data(sid):
    book.sheets[sid].select()
    rowcount=book.sheets[sid].range(book.sheets[sid].cells(1,1)).end('down').row
    s=Session()
    for rowid in range(2,rowcount+1):
        mid=int(book.sheets[sid].cells(rowid,1).value)
        matchobj = s.query(match).filter(match.id == mid).one()
        print([mid,rowid])
        basedata=[matchobj.league.name_short,matchobj.kickoff,matchobj.home.name,matchobj.away.name,matchobj.score_full,matchobj.score_half]
        book.sheets[sid].cells(rowid,2).value=basedata
        book.sheets[sid].range(book.sheets[sid].cells(rowid,8),book.sheets[sid].cells(rowid,14)).clear()
        book.sheets[sid].cells(rowid,8).value=get_close_odds(mid)
    s.close()
book=xw.books['nowgoal_xx.xlsx']
print('start')
# get_close_odds(2159085,debug=True)
refresh_sheet_data(0)

