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
                UniprotAccession TEXT PRIMARY KEY, 
                Source TEXT, 
                EntryName TEXT, 
                Annotation TEXT, 
                Organism TEXT, 
                OrganismID TEXT, 
                GeneName TEXT, 
                ProteinExistence TEXT, 
                SequenceVersion TEXT, 
                Sequence TEXT)''')

