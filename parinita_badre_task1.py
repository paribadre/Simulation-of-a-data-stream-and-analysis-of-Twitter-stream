import random
import binascii
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
import sys

port_number = int(sys.argv[1])
output_file_path = sys.argv[2]

# port_number = 9999
# output_file_path = 'task1_output.txt'

sc = SparkContext("local[*]", "DMAssignment 5")
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 10)

with open(output_file_path, 'w') as fout:
    fout.write('Time,FPR')
fout.close()

true_visited = set()
m = 200
filter_bit_array = []
for x in range(0, m):
    filter_bit_array.append(0)

hash_functions_list = []
for i in range(0, 2):
    individual_hash = []
    for j in range(0, 2):
        k = random.randint(1, 100)
        individual_hash.append(k)
    hash_functions_list.append(individual_hash)

false_positive = 0
true_negative = 0
fpr = []


def writetofile(timestamp, stats):
    f = open(output_file_path, 'a')
    f.write("\n"+str(timestamp)+","+str(stats))


def get_hash(state_val):
    hash_values = []
    for i in hash_functions_list:
        val = ((i[0] * state_val) + i[1]) % m
        hash_values.append(val)
    return hash_values


def bloom_filter(time, rdd):
    global false_positive, true_negative

    for i in rdd.collect():
        # Get the integer form of state
        check = 0
        state = json.loads(i)['state'].encode('utf-8')
        state_num = int(binascii.hexlify(state), 16)

        hash_vals = get_hash(state_num)

        # Check if all are 1
        for i in hash_vals:
            if filter_bit_array[i] == 1:
                check += 1

        if check == len(hash_vals):
            if state not in true_visited:
                false_positive += 1
        else:
            true_negative += 1

        # Update visited set and bloom filter
        true_visited.add(state)
        for i in hash_vals:
            filter_bit_array[i] = 1

    fpr = float(false_positive / (false_positive + true_negative))
    writetofile(time, fpr)


lines = ssc.socketTextStream("localhost", port_number)
lines.foreachRDD(bloom_filter)

ssc.start()
ssc.awaitTermination()




