import time

def getTimeWithPrefix(val):
    if val< 10:
        return "0"+str(val)
    return str(val)
def getCurrentTimeStampKey():
    localtime = time.localtime(time.time())
    #print(localtime[0], localtime[1])
    res = ""+getTimeWithPrefix(localtime[2])+getTimeWithPrefix(localtime[1])+getTimeWithPrefix(localtime[3])+getTimeWithPrefix(localtime[4])
    #print(res)
    return res
def getCurrentTimeStamp():
    localtime = time.localtime(time.time())
    res = ""+getTimeWithPrefix(localtime[2])+getTimeWithPrefix(localtime[1])+getTimeWithPrefix(localtime[0])+getTimeWithPrefix(localtime[3])+getTimeWithPrefix(localtime[4])+getTimeWithPrefix(localtime[5])
    return res