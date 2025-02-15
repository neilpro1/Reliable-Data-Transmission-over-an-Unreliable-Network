import sys
from socket import *
import threading
import time
import queue
import pickle
import random
import os

def sendAck( ackNo, sock, end ):
    rand = random.randint(0,9)
    if rand > 1:
        toSend = (ackNo,)
        msg = pickle.dumps( toSend)
        sock.sendto( msg, end)

def rx_thread( s, sender, que, bSize):
    expected_block = 1

    while True:
        req,_ = s.recvfrom(bSize + 64)
        reply = pickle.loads(req)
        ackNo = reply[0]
        print(f"Received reply: code = {ackNo} expected_block = {expected_block}")

        if ackNo == expected_block:
            data = reply[1]
            que.put(data)
            sendAck(expected_block, s, sender)
            expected_block += 1
            if(len(data) < bSize):
                break
        else:
            sendAck(expected_block-1, s, sender)
    return
    
def receiveNextBlock( q ):
    return q.get()

def main(sIP, sPort, fNameRemote, fNameLocal, chunkSize):

    s = socket( AF_INET, SOCK_DGRAM)
    #interact with sender without losses
    request = (fNameRemote, chunkSize)
    req = pickle.dumps(request)
    sender = (sIP, sPort)
    print("sending request")
    s.sendto( req, sender)
    print("waiting for reply")
    rep, ad = s.recvfrom(128)
    reply = pickle.loads(rep)
    print(f"Received reply: code = {reply[0]} fileSize = {reply[1]}")
    if reply[0]!=0:
        print(f'file {fNameRemote} does not exist in sender')
        sys.exit(1)
    #start transfer with data and ack losses
    fileSize = reply[1]

    # falta testar se existe o ficheiro local
    if os.path.exists(fNameLocal) and os.path.getsize(fNameLocal) == fileSize:
        print(f'file {fNameLocal} already exist on receiver')
        reply = (1, 0)
        req = pickle.dumps(reply)
        s.sendto(req, sender)
        sys.exit(1)
    else:
        print("start!")
        reply = (0, 0)
        req = pickle.dumps(reply)
        s.sendto(req, sender)

    q = queue.Queue( )
    tid = threading.Thread( target=rx_thread, args=(s, sender, q, chunkSize))
    tid.start()
    f = open( fNameLocal, 'wb')
    noBytesRcv = 0
    while noBytesRcv < fileSize:
        print(f'Going to receive; noByteRcv={noBytesRcv}')
        b = receiveNextBlock( q )
        sizeOfBlockReceived = len(b)
        if sizeOfBlockReceived > 0:
            f.write(b)
            noBytesRcv += sizeOfBlockReceived

    f.close()
    tid.join()
    print("Sucess!")

       

if __name__ == "__main__":
    # python receiver.py senderIP senderPort fileNameInSender fileNameInReceiver chunkSize
    if len(sys.argv) != 6:
        print("Usage: python receiver.py senderIP senderPort fileNameRemote fileNameLocal chunkSize")
        sys.exit(1)
    senderIP = sys.argv[1]
    senderPort = int(sys.argv[2])
    fileNameRemote = sys.argv[3]
    fileNameLocal = sys.argv[4]
    chunkSize = int(sys.argv[5])
    main( senderIP, senderPort, fileNameRemote, fileNameLocal, chunkSize)
    
