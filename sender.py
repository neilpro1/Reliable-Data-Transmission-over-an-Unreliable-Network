import sys
import os
from socket import *
import threading
import time
import queue
import pickle
import random
import select

blocksInWindow = 0
window = []

def sendDatagram( blockNo, contents, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (blockNo, contents)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)
        
def waitForAck( s, seg ):
    rx, tx, er = select.select( [s], [],[], seg)
    return rx!=[]


def tx_thread( s, receiver, windowSize, cond, timeout ):
    global blocksInWindow, window

    while True:
        waitAnswer = waitForAck(s, timeout)

        cond.acquire()

        if blocksInWindow == 0 and not waitAnswer:
            break;

        if not waitAnswer:
            for block in window:
                ackNo = block[0]
                data = block[1]
                sendDatagram(ackNo, data, s, receiver)
        else:
            buf,_ = s.recvfrom(64)
            req = pickle.loads(buf)
            ackNo = req[0]

            while window and window[0][0] <= ackNo:
                print(f'block sent successfully: {window[0][0]}')
                window.pop(0)
                blocksInWindow -= 1
                cond.notify()
        cond.release()

    return 

def sendBlock( seqNo, fileBytes, s, receiver, windowSize, cond ):  #producer

    cond.acquire()
    global blocksInWindow, window

    if blocksInWindow >= windowSize:
        cond.wait()

    window.append((seqNo, fileBytes))
    blocksInWindow += 1
    sendDatagram(seqNo, fileBytes, s, receiver)
    cond.release()
    return

def main(hostname, senderPort, windowSize, timeOutInSec):
    s = socket( AF_INET, SOCK_DGRAM)
    s.bind((hostname, senderPort))
    # interaction with receiver; no datagram loss
    buf, rem = s.recvfrom( 256 )
    req = pickle.loads( buf)
    fileName = req[0]
    blockSize = req[1]
    result = os.path.exists(fileName)
    if not result:
        print(f'file {fileName} does not exist in server')
        reply = ( 1, 0 )
        rep=pickle.dumps(reply)
        s.sendto( rep, rem )
        sys.exit(1)
    fileSize = os.path.getsize(fileName)
    reply = ( 0, fileSize)
    rep=pickle.dumps(reply)
    s.sendto( rep, rem )

    buf, _ = s.recvfrom(256)
    replay = pickle.loads(buf)
    if replay[0] != 0:
        print(f'file already exist on receiver {replay[0]}')
        sys.exit(1)

    # file transfer; datagram loss possible
    windowCond = threading.Condition()
    tid = threading.Thread( target=tx_thread,
                            args=(s,rem,windowSize, windowCond,timeOutInSec))
    tid.start()
    f = open( fileName, 'rb')
    blockNo = 1
    
    while True:
        b = f.read( blockSize  )
        sizeOfBlockRead = len(b)
        if sizeOfBlockRead > 0:
            sendBlock( blockNo, b, s, rem, windowSize, windowCond)
        if sizeOfBlockRead == blockSize:
            blockNo=blockNo+1
        else:
            break
    f.close()
    tid.join()
    print("Sucess!")


if __name__ == "__main__":
    # python sender.py senderPort windowSize timeOutInSec
 

    if len(sys.argv) != 4:
        print("Usage: python sender.py senderPort windowSize timeOutInSec")
    else:
        senderPort = int(sys.argv[1])
        windowSize = int(sys.argv[2])
        timeOutInSec = int(sys.argv[3])
        hostname = gethostbyname(gethostname())
        main( hostname, senderPort, windowSize, timeOutInSec)
