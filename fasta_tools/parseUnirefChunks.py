'''>UniRef50_Q6GZX4 Putative transcription factor 001R n=47 Tax=root TaxID=1 RepID=001R_FRG3G   MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNPPS    EKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVLSDLD    AKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKRVDIQHL    EKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTGSKGVLYDD    SFRKIYTDLGWKFTPL'''

import multiprocessing as mp
import sqlite3
import sys
import re

con = sqlite3.connect('/scratch/06538/mschecht/Databases/ProteinSequences.db')
tableName = 'Uniref50'
fileBase = f'uniref50_chunk_'
fileNames = []
dataDir = '/scratch/06538/mschecht/FastaChunking/uniref50_chunks/'
start = int(sys.argv[1])
stop = int(sys.argv[2])

annotationRegex = re.compile('(?<=\ )(.*)(?=\ n=)')
clusterSizeRegex = re.compile('(?<=n=)(\d+)(?=\ Tax=)')
commonTaxonRegex = re.compile('(?<=Tax=)(.*)(?=\ TaxID=)')
commonTaxonIDRegex = re.compile('(?<=TaxID=)(\d+)(?=\ RepID=)')
representativeIDRegex = re.compile('(?<=RepID=)(.*)')

for i in range(start,stop+1):
    fileNames.append(f'{dataDir}{fileBase}{i}.fasta')

def createTable():
    db = con.cursor()
    db.execute(f'''CREATE TABLE IF NOT EXISTS {tableName} (
                UnirefID TEXT PRIMARY KEY,
                Annotation TEXT,
                ClusterSize TEXT,
                CommonTaxon TEXT,
                CommonTaxonID TEXT,
                RepresentativeID TEXT,
                Sequence TEXT)''')

def fastaRegex(header):
    unirefID = header.split(' ')[0][1:]
    annotation = re.search(annotationRegex, header)[0] if re.search(annotationRegex, header) else 'NULL'
    clusterSize = re.search(clusterSizeRegex, header)[0] if re.search(clusterSizeRegex, header) else 'NULL'
    commonTaxon = re.search(commonTaxonRegex, header)[0] if re.search(commonTaxonRegex, header) else 'NULL'
    commonTaxonID = re.search(commonTaxonIDRegex, header)[0] if re.search(commonTaxonIDRegex, header) else 'NULL'
    representativeID = re.search(representativeIDRegex, header)[0] if re.search(representativeIDRegex, header) else 'NULL'

    return (unirefID, annotation, clusterSize, commonTaxon, commonTaxonID, representativeID)

def parseFile(fileName):
    data = []
    with open(fileName) as file:
        for line in file:
            header = line.split('\t')[0]
            seqList = line.split('\t')[1:-1]
            sequence = ''.join(seqList)
            unirefID, annotation, clusterSize, commonTaxon, commonTaxonID, representativeID = fastaRegex(header)
            data.append((unirefID, annotation, clusterSize, commonTaxon, commonTaxonID, representativeID, sequence))
    return data

if __name__ == '__main__':
    createTable()
    fastaInserted = 0

    physicalCores = int(mp.cpu_count()/2)
    p = mp.Pool(physicalCores)
    multiResults = p.imap(parseFile, fileNames)

    for result in multiResults:
        db = con.cursor()
        db.executemany(f'''INSERT INTO {tableName}
            (UnirefID, Annotation, ClusterSize, CommonTaxon, CommonTaxonID, RepresentativeID, Sequence)
            VALUES (?,?,?,?,?,?,?)''', result)
        con.commit()
        fastaInserted += len(result)
        print(f'Inserted {fastaInserted} entries into db')
    print(f"Finished parsing chunks {start} through {stop}\n")
