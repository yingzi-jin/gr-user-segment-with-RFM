#/usr/bin/env python
# -*- coding: utf-8 -*-

def get_list(FIN,HEAD="F"):
    outlist=[]
    f = open(FIN,"r")
    k=0
    for line in f:
        k += 1
        ## 最初の行がheadである場合(HEAD="T")は飛ばす
        if HEAD == "T" and k==1: continue
        line = line.rstrip()
        outlist.append(line)
    return outlist


mydic={}
mylist = get_list("./tmp.txt")
for lt in mylist:
    lt = lt.replace("[","")
    lt = lt.replace("]","")
    apps = lt.split(",")
    print apps
    for i in range(len(apps)):
        app = apps[i]
        if app not in mydic:
            mydic[app] = 0
        v = 1*1.0/(i+1)
        #print app,v
        mydic[app] += v

x=0
for k,v in sorted(mydic.items(),key=lambda x:x[1], reverse=True):
    x +=1
    #if x<=11:
    print k+",",
