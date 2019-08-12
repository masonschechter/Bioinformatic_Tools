'''>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1       MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNPPS     EKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVLSDLD    AKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKRVDIQHL    EKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTGSKGVLYDD     SFRKIYTDLGWKFTPL\n'''

import multiprocessing as mp
import sqlite3
import sys
import re

con = sqlite3.connect('ProteinSequences.db')

start = int(sys.argv[1])
stop = int(sys.argv[2])
fileBase = 'uniparc_sprot_chunk_'
fileNames = []

annotationRegex = re.compile('(?<=\ )(.*)(?=\ OS=)')
organismRegex = re.compile('(?<=OS=)(.*)(?=\ OX=)')
organismIDRegex = re.compile('(?<=OX=)(\d+)')
geneNameRegex = re.compile('(?<=GN=)(.*)(?=\ PE=)')
proteinExistenceRegex = re.compile('(?<=PE=)(\d+)')
sequenceVersionRegex = re.compile('(?<=SV=)(\d+)')

for i in range(start,stop+1):
    fileNames.append(f'{fileBase}{i}.fasta')

def createTable():
    db = con.cursor()
    db.execute('''CREATE TABLE IF NOT EXISTS UniprotSP (
                UniprotID TEXT PRIMARY KEY, 
                Source TEXT, 
                EntryName TEXT, 
                Annotation TEXT, 
                Organism TEXT, 
                OrganismID TEXT, 
                GeneName TEXT, 
                ProteinExistence TEXT, 
                SequenceVersion TEXT, 
                Sequence TEXT)''')

def fastaRegex(header):
    processedHeader = header.split(' ')[0].split('|')
    uniprotID = processedHeader[1]
    source = processedHeader[0][1:]
    entryName = processedHeader[2]
    annotation = re.search(annotationRegex, header)[0] if re.search(annotationRegex, header) else 'NULL'
    organism = re.search(organismRegex, header)[0] if re.search(organismRegex, header) else 'NULL'
    organismID = re.search(organismIDRegex, header)[0] if re.search(organismIDRegex, header) else 'NULL'
    geneName = re.search(geneNameRegex, header)[0] if re.search(geneNameRegex, header) else 'NULL'
    proteinExistence = re.search(proteinExistenceRegex, header)[0] if re.search(proteinExistenceRegex, header) else 'NULL'
    sequenceVersion = re.search(sequenceVersionRegex, header)[0] if re.search(sequenceVersionRegex, header) else 'NULL'

    return (uniprotID, source, entryName, annotation, organism, organismID, geneName, proteinExistence, sequenceVersion)

def parseFile(fileName):
    data = []
    with open(fileName, 'r') as file:
        for line in file:
            header = line.split('\t')[0]
            seqList = line.split('\t')[1:-1]
            sequence = ''.join(seqList)
            uniprotID, source, entryName, annotation, organism, organismID, geneName, proteinExistence, sequenceVersion = fastaRegex(header)
            data.append((uniprotID, source, entryName, annotation, organism, organismID, geneName, proteinExistence, sequenceVersion, sequence))
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
            db.executemany('''INSERT INTO UniprotSP 
                (UniprotID, Source, EntryName, Annotation, Organism, OrganismID, GeneName, ProteinExistence, SequenceVersion, Sequence) 
                VALUES (?,?,?)''', result)
            con.commit()
            fastaInserted += 100000
            print(f'Inserted {fastaInserted} entries into db')
        else:
            print(f"we dun goofed.")
    print(f"Finished parsing chunks {start} through {stop}\n")

