import sys
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
import operator
import random
import numpy
import time

def main():

    #basic checks here
    if len(sys.argv)!=3 and len(sys.argv)!=6:
        return
        

    #creating two znodes that maintain the frequent and recent lists
    #need to create watchers for the znodes inorder to display

    #create a client for the zookeeper server
    ipaddress = sys.argv[1]
    zk = KazooClient(hosts=ipaddress)
    print ipaddress

    name = sys.argv[2]

    zk.start()

    #handle errors here
    if zk.exists("/"+name+"ephermal"):
        print "Player with same name is already online"
        return


    #create a new znode if its is not present
    zk.ensure_path('/'.join(name))
    zk.ensure_path('/recent')
    zk.ensure_path("/frequent")
    zk.create("/"+name+"ephermal", b"some value", None, True)

    if len(sys.argv)==6:
        generateScores(zk,name)
        return
    
    #take scores synchronously from the user
    i=0
    prev=''
    prevs=''
    while True:
        score = raw_input("Enter score: ")
        
        #set the scores in original znode
        val='/'.join(name)
        print score
        #zk.set(val,bytes(str(score), 'ascii'))

        #set append the new scores to original string
        data, stat = zk.get('/recent')
        prev= name+':'+score+'#'+data

        #set append to new scores in frequent list
        data, stat = zk.get('/frequent')
        prevs= name+':'+score+'#'+data

        # zk.set('/recent',prev.encode())
        # zk.set('/frequent',prevs.encode())
        setrecent(zk,prev)
        setfrequent(zk,prevs)
        i+=1

    #print prev
    #print prevs


    zk.stop()

def setrecent(zk,final):
    data = final.split('#')
    #print data
    i=0
    n=''
    for temp in data:
        if temp.lstrip() is not '' and i<25:
            n=n+temp+'#'+' '
            i+=1
    #print n
    zk.set('/recent',n.encode())

def setfrequent( zk, data ):
    d = data.split('#') #split string into a list
    
    i=0
    n=''
    #print(d)
    a_list=[]
    for temp in d:
        if temp.lstrip() is not '':
            name=temp.split(":")
            name[1] = int(name[1])
            #print(tuple(name))
            a_list.append(tuple(name))
    
    #print a_list
    
    a_list.sort(key=operator.itemgetter(1))    
    
    #from last get 25 sorted pairs and update
    i=0
    l=''
    for t in reversed(a_list):
        if i<25:
            l+=t[0]+':'+str(t[1])+'#'+' '
        i+=1
    #print l
    zk.set('/frequent',l.encode())


def generateScores(zk,name):
    num= int(sys.argv[3])
    udelay = int(sys.argv[4])
    uscore= int(sys.argv[5])

    lis=numpy.random.normal(uscore,int(uscore/3),num)
    listime=numpy.random.normal(udelay,int(udelay/3),num)

    i=0
    for temp in lis:
        tim=int(listime[i])
        print "Score:   "+str(int(temp))
        #set append the new scores to original string
        data, stat = zk.get('/recent')
        prev= name+':'+str(int(temp))+'#'+data

        #set append to new scores in frequent list
        data, stat = zk.get('/frequent')
        prevs= name+':'+str(int(temp))+'#'+data

        setrecent(zk,prev)
        setfrequent(zk,prevs)

        i+=1
        time.sleep(tim)







if __name__== "__main__":
    print "Hello"
    main()
