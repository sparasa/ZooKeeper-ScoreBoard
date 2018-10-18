import sys
from kazoo.client import KazooClient
ipaddress = sys.argv[1]
zk = KazooClient(hosts=ipaddress)
zk.start()

recent = sys.argv[2]
frequent = sys.argv[3]


@zk.DataWatch("/recent")
def watch_node(data, stat):
    final = data.split('#') #split string into a list
    i=0
    n=''
    for temp in final:
        if temp is not ' ' and i<int(recent):
            l=temp.split(':')
            name=l[0].lstrip()
            score=l[1]
            if zk.exists("/"+name+"ephermal"):
                print name+"        "+score+"    "+"***"
            else:
                print name+"        "+score
            i+=1
    print ""
    print ""

@zk.DataWatch("/frequent")
def watch_node(data, stat):
    d = data.split('#') #split string into a list

    i=0
    n=''
    for temp in d:
        if temp is not ' ' and i<int(frequent):
            l=temp.split(':')
            name=l[0].lstrip()
            score=l[1]
            if zk.exists("/"+name+"ephermal"):
                print name+"        "+score+"    "+"***"
            else:
                print name+"        "+score
            i+=1
    print ""
    print ""

while True:
    score = raw_input("")