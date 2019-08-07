import subprocess
import sys

inFile = sys.argv[1]

result = subprocess.Popen(f'wc -l {inFile}', stdout=subprocess.PIPE).communicate()
lineCount = int(result[0].decode('utf-8').split(' ')[0])

print(f'Line count: {lineCount}')

lineStarts = []
chunkSizes = []
data = []

with open(inFile, 'r') as file:
	currentLine = 0
	for line in file:
		if line.startswith('>'):
			lineStarts.append(currentLine)
		currentLine += 1

print(f'Found {len(lineStarts)} fasta entries in the file!')

for i in range(len(lineStarts)):
	try:
		chunkSize = lineStarts[i+1] - lineStarts[i]
	except:
		chunkSize = lineCount - lineStarts[i]
	chunkSizes.append(chunkSize)

print('Done calculating chunk sizes!')

fileNumber = 1
fastaInserted = 0

with open(inFile, 'r') as file:
	for chunkSize in chunkSizes:
		fastaEntry = []
		for i in range(chunkSize):
			fastaEntry.append(file.readline().strip())
		else:
			data.append(fastaEntry)
		if len(data) == 100:
			with open(f'ecoli_{fileNumber}', 'w') as outFile:
				for chunk in data:
					for _ in chunk:
						outFile.write(_+'\t')
					else:
						outFile.write('\n')
			data = []
			fileNumber += 1
	else:
		with open(f'ecoli_{fileNumber}', 'w') as outFile:
			for chunk in data:
				for _ in chunk:
					outFile.write(_+'\t')
				else:
					outFile.write('\n')






