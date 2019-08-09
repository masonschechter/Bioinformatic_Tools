'''>UPI0000000011 status=active    MVDAITVLTAIGITVLMLLMVISGAAMIVKELNPNDIFTMQSLKFNRAVTIFKYIGLFIY    IPGTIILYATYVKSLLMKS \n
uniparc_active_chunk_#.fasta 2819 total'''

import multiprocessing as mp
import sqlite3
import sys

statusDict = {"active":1, "inactive":0}
con = sqlite3.connect('ProteinSequences.db')

start = int(sys.argv[1])
stop = int(sys.argv[2])
fileBase = 'uniparc_active_chunk_'
fileNames = []


for i in range(start,stop+1):
    fileNames.append(f'{fileBase}{i}.fasta')

def createTable():
    db = con.cursor()
    db.execute("CREATE TABLE IF NOT EXISTS Uniparc (UniparcID, Status, Sequence)")


def parseFile(fileName):
    data = []
    with open(fileName, 'r') as file:
        for line in file:
            uniparcID = line.split(' ')[0][1:]
            status = statusDict[line.split('\t')[0].split(' ')[1].split('=')[1]]
            sequence = ''
            for _ in line.split('\t')[1:-1]:
                sequence += _
            data.append((uniparcID, status, sequence))
    return data

if __name__ == '__main__':
    createTable()
    fastaInserted = 0

    physicalCores = int(mp.cpu_count()/2)
    p = mp.Pool(physicalCores)
    multiResults = p.imap(parseFile, fileNames)

    for result in multiResults:
        if not len(result) % 100000:
            db = con.cursor()
            db.executemany("INSERT INTO Uniparc (UniparcID, Status, Sequence) VALUES (?,?,?)", result)
            con.commit()
            fastaInserted += 100000
            print(f'Inserted {fastaInserted} entries into db')