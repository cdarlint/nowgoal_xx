import nowgoal_inplay as n
from nowgoal_inplay.actions import refreshSeasonNames, refreshSeasonMeta, getOpenCloseOdds

s=n.Session()
r1=s.query(n.league).all()
print(r1)
print(r1[0].name_short)#BD1

refreshSeasonNames('BD1')
s.close()
s=n.Session()
r2=s.query(n.league).filter(n.league.name_short=='BD1').one()
print(r2.seasons)
print(r2.seasons[0].name) # '2022'
print(r2.seasons[0].matches)
refreshSeasonMeta('BD1','2022')
s.close()
s=n.Session()
r3=s.query(n.league).filter(n.league.name_short=='BD1').one()
print(r3.seasons[0].matches)
scored=list(filter(lambda x:x.score_full!='',r3.seasons[0].matches))
print(len(scored))
cnt=0
for match in scored:
    cnt+=1
    mid=match.id
    print(cnt)
    getOpenCloseOdds(mid)
for match in scored:
    print(match.id)