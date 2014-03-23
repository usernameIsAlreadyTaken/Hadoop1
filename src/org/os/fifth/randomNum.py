import sys
import random

output = open(sys.argv[1], "w")
lines = int(sys.argv[2])

def getNumbers():
	numbers = [];
	for i in xrange(lines):
        	numbers.append(str(random.randint(1,999)) + "\n");
	return numbers;

output.writelines(getNumbers());
output.close()

