import json
import re
from os.path import exists
# import os
from io import StringIO
from bs4 import BeautifulSoup
from dateutil import tz
from datetime import datetime
from .orm import league, bookmaker, season, Session, team, match, had, asian, hilo
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload
from urllib.parse import urlparse
from pathlib import PureWindowsPath, Path
import requests
import json

# path_prefix=PureWindowsPath('mock_data')
path_prefix = Path('mock_data')


def refreshSeasonMeta(league, season, force_remote=False):
    # parse
    data=getSeasonMeta(league,season,force_remote=force_remote)
    # teams
    s=Session()
    for teamrow in data['arrTeam']:
        try:
            s.query(team.id).filter(team.id==teamrow[0]).one()
        except NoResultFound:
            teamobj=team(id=teamrow[0],name=teamrow[3],namecn=teamrow[4],pic_url=teamrow[5])
            s.add(teamobj)
            print('team added: ',teamobj)
    s.commit()
    # round-matches
    seasonid=_data_season(league,data['arrLeague'][4]).id
    leagueid=data['arrLeague'][0]
    for round in data['rounds']:
        for matchrow in data['rounds'][round]:
            # print(matchrow)
            # match_scores=matchrow[6].split('-')+matchrow[7].split('-')
            # check existing, update / replace
            # TODO: update half/full score, other than only kickoff time
            #  try compare existing with new ORM instance
            try:
                matchobj=s.query(match).filter(match.id==matchrow[0]).one()
                ko=datetime.strptime(matchrow[3],'%Y-%m-%d %H:%M')
                if matchobj.kickoff==ko:
                    pass
                else:
                    print(f'match {matchrow[0]} kickoff changed to {ko.strftime("%Y-%m-%d %H:%M")}')
                    matchobj.kickoff=ko
                if matchobj.score_full is None or matchobj.score_half is None \
                        or matchobj.score_half!=matchrow[7]\
                        or matchobj.score_full!=matchrow[6]:
                    print(f'fill score: H {matchrow[7]} / F {matchrow[6]}')
                    matchobj.score_half=matchrow[7]
                    matchobj.score_full=matchrow[6]
            # no exist, insert
            except NoResultFound as ex:
                matchobj=match(id=matchrow[0],league_id=leagueid,season_id=seasonid,round_id=round,
                               kickoff=datetime.strptime(matchrow[3],'%Y-%m-%d %H:%M'),
                               home_id=matchrow[4],away_id=matchrow[5],
                               score_half=matchrow[7],score_full=matchrow[6])
                s.add(matchobj)

    s.commit()


    s.close()


def getSeasonMeta(league, season, force_remote=False):
    meta = {}
    round = {}
    so=_data_season(league,season)
    data=getResource(f'https://info.nowgoal5.com/jsData/matchResult/{so.name}/s{so.league.id}_en.js',force_remote=force_remote)
    for row in data.splitlines():
        if row.startswith('var'):
            m = re.fullmatch(r'var ([a-zA-Z]+) ?\= ?(.*);', row.strip())
            m = list(m.groups())
            m[1]=json.loads(m[1].replace("'",'"').replace('&nbsp;',' '))
            meta.update({m[0]: m[1]})
        elif row.startswith('jh'):
            m = re.fullmatch(r'jh\[\"R_(\d{1,2})\"\] \= ?(.*);', row.strip())
            m = list(m.groups())
            m[1]=json.loads(m[1].replace("'",'"').replace(',,',',"",').replace(',,',',"",'))
            round.update({int(m[0]): m[1]})
        else:
            pass
    meta.update({'rounds': round})
    return meta


def refreshOdds(matchid, bookmaker, force_remote=False):
    # TODO: check match exist, as well as season/league

    # check existing odds
    bookmakerid=_data_bookmaker(bookmaker).id
    existing=_data_match_odds(matchid,bookmakerid)
    def prod(iterable):
        p = 1
        for n in iterable:
            p *= n
        return p
    # non-exist => refresh; exist & force => refresh
    if sum(len(existing[otype]) for otype in existing.keys())==0:
    #     or (prod(len(existing[otype]) for otype in existing.keys())>0 and force_remote):

        odds=getOdds(matchid,bookmaker,force_remote=force_remote)
        s=Session()
        try:
            [s.add(o) for ot in odds.keys() for o in odds[ot]]
            # for ot in odds.keys():
            #     for o in odds[ot]:
            #         s.add(o)
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    # # exist & not force => do nothing
    # # TODO: and, give WARNING
    # elif (prod(len(existing[otype]) for otype in existing.keys())>0 and force_remote==False):
    #     pass
    # else:
    #     # TODO: partial in original html, fully imported
    #     raise BaseException(f'match odds partial exist in db, mid {matchid}, bmid {bookmakerid}')


# none/ L / R /both
# 518 522 504 348
def getOdds(matchid, bookmaker,force_remote=False):
    # data = open('mock_data/3in1odds/3_1552348.html', 'rt', encoding='utf8').read()
    # print(len(data))
    bookmakerid=_data_bookmaker(bookmaker).id
    data=getResource(f'http://data.nowgoal5.com/3in1odds/{bookmakerid}_{matchid}.html',force_remote=force_remote)
    # html = BeautifulSoup(data, 'lxml')
    # tbs={}
    # oddsTypes={'div_h':had,'div_l':asian,'div_d':hilo}
    # bookmaker_id=_data_bookmaker(bookmaker).id
    # for tablename in ('div_h','div_l','div_d'):
    #     tb=html.select('#'+tablename+' table')[0].select('tr')[2:]
    #     # for tr in tb:
    #     #     print(_odds_row_parse(tr))
    #     tbs.update({tablename:_odds_table_parse(tb,oddsType=oddsTypes[tablename],matchid=matchid,bookmakerid=bookmaker_id)})
    # return tbs


def getOpenCloseOdds(mid):
    localpath = path_prefix / 'odds_open_close' / f'{mid}.txt'
    data = getResource(
        f'https://www.nowgoal5.com/Ajax/SoccerAjax?type=4&id={mid}',
        localpath=localpath, timeout=3)
    return data


def refreshSeasonNames(league, force_remote=True):
    # http://info.nowgoal.com/jsData/LeagueSeason/sea36.js
    leagueid=_data_league(league).id
    seasons_row=getResource(f'http://info.nowgoal5.com/jsData/LeagueSeason/sea{leagueid}.js',force_remote=force_remote)
    seasonslist=re.fullmatch(r'.*= \[(\'.*\',?)+\];',seasons_row.strip()).group(1).split(',')
    seasonslist=[s[1:-1] for s in seasonslist]
    s=Session()
    existing=s.query(season).filter(season.league_id==leagueid)
    exlist = [e.name for e in existing.all()]
    for r in seasonslist:
        if r not in exlist:
            s.add(season(name=r,league_id=leagueid))
    try:
        s.commit()
    except Exception as x:
        s.rollback()
    s.close()


def getResource(url, force_remote=False, localpath=None,timeout=10):
    # locate local file, before request remote
    # TODO: deal with 404
    urlpath = urlparse(url).path
    if not localpath:
        localpath = path_prefix / urlpath[1:]
    if not force_remote and exists(localpath):
        with open(localpath, 'rb') as f:
            data = f.read()
    else:
        localpath.parent.mkdir(parents=True, exist_ok=True)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        data = requests.get(url, headers=headers,timeout=timeout).content
        with open(localpath, 'wb') as f:
            f.write(data)
    return data.decode('utf-8-sig')


def _local_cache_path(localpath):
    if exists(localpath):
        return localpath
    else:
        return None


def _data_season(leaguestr,season_str):
    leagueid=_data_league(leaguestr).id
    if type(season_str)==type('str'):
        s=Session()
        try:
            sobj=s.query(season).options(joinedload(season.league)).filter(season.league_id==leagueid).filter(season.name==season_str).one()
            return sobj
        except NoResultFound as nr:
            refreshSeasonNames(leaguestr)
            try:
                sobj=s.query(season).options(joinedload(season.league)).filter(season.league_id==leagueid).filter(season.name==season_str).one()
            except NoResultFound:
                raise BaseException(f'no season named {season_str} in {leaguestr}')
            return sobj
        finally:
            s.close()
    raise


def _data_league(leaguestr):
    if type(leaguestr) == type('str') and len(leaguestr) == 3:
        s = Session()
        leagueobj = s.query(league).filter(league.name_short == leaguestr).one()
        s.close()
        return leagueobj
    else:
        raise BaseException(f'league error: {leaguestr}')


def _data_bookmaker(bookmakerstr):
    if type(bookmakerstr) == type('str'):
        s = Session()
        bmobj = s.query(bookmaker).filter(bookmaker.name == bookmakerstr).one()
        s.close()
        return bmobj
    else:
        raise BaseException(f'bookmaker error: {bookmakerstr}')


def _data_match_odds(matchid,bookmakerid):
    s=Session()
    otypes={'had':had,'asian':asian,'hilo':hilo}
    odds={oname:s.query(otype).filter(otype.match_id==matchid).filter(otype.bookmaker_id==bookmakerid).all()
          for oname,otype in otypes.items()}

    s.close()
    return odds


def _odds_table_parse(tb,oddsType,matchid,bookmakerid):
    trs=[_odds_row_parse(tr) for tr in tb]
    objs=[oddsType(closed=True,match_id=matchid,bookmaker_id=bookmakerid,gametime=tr[0],foul_home=tr[1],foul_away=tr[3],score=tr[2],updated=tr[7],stage=tr[8])
          if 'Closed' == tr[5] else
          oddsType(closed=False, match_id=matchid, bookmaker_id=bookmakerid, gametime=tr[0], o1=tr[4], o2=tr[5], o3=tr[6],foul_home=tr[1], foul_away=tr[3], score=tr[2], updated=tr[7], stage=tr[8])
          for tr in trs]
    return objs


def _odds_row_parse(tr):
    tds=tr.select('td')
    _min=tds[0].text
    # if closed, deal with it in next step: _odds_table_parse
    o1=tds[2].text
    o2=tds[3].text
    o3=tds[4].text
    ts=_day_local(tds[5])
    status=tds[6].text.strip()
    return [_min,*_score_with_card(tds[1]),o1,o2,o3,ts,status]



def _score_with_card(td):
    elems = td.find_all(text=True)
    a = b = c = 0
    if len(elems) == 1:
        b = elems[0]
    elif len(elems) == 3:
        a = elems[0]
        b = elems[1]
        c = elems[2]
    elif elems[0].findParent().name == 'td':
        b = elems[0]
        c = elems[1]
    else:
        a = elems[0]
        b = elems[1]
    return int(a), str(b), int(c)


# http://data.nowgoal.com/js/func.js
# function showDate(t0,t1,t2,t3,t4,t5)
# {
# 	var t = new Date(t0,t1,t2,t3,t4,t5);
# 	t = new Date(Date.UTC(t.getFullYear(),t.getMonth(),t.getDate(),t.getHours(),t.getMinutes(),t.getSeconds()));
#     var y=t.getFullYear();
#     var M=t.getMonth()+1;
#     var d=t.getDate();
#     var h=t.getHours();
#     var m=t.getMinutes();
#     if(M<10) M="0" + M;
#     if(d<10) d="0" + d;
#     if(h<10) h="0" + h;
#     if(m<10) m="0" + m;
#     document.write(M+"-" + d +" " + h +":" + m);
# }
# td=g[2].find_all('td')[5]
def _day_local(td):
    raw = re.fullmatch('showDate\((.*)\)', td.findChild('script').contents[0]).group(1)
    return datetime.strptime(raw, '%Y,%m-1,%d,%H,%M,%S').replace(tzinfo=tz.tzutc()).astimezone(tz.gettz('PRC'))
