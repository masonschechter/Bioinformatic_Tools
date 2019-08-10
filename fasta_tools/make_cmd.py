'''cat the output file into the .slurm'''

numFiles = 2819
batchSize = 24
remainder = numFiles % batchSize
highestMultiple = (numFiles - remainder)//batchSize

with open('parseUniparc.cmd', 'w') as file:
    for i in range(highestMultiple):
        start = i*batchSize+1
        stop = (i+1)*batchSize
        file.write(f"python3 parseUniparcChunks.py {start} {stop}\n")
    file.write(f"python3 parseUniparcChunks.py {highestMultiple*batchSize+1} {numFiles}\n")