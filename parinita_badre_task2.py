from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import json
import binascii
import math

port_number = int(sys.argv[1])
output_file_path = sys.argv[2]
# port_number = 9999
# output_file_path = "task2_output.csv"

f = open(output_file_path, "w")
f.write("Time,Ground Truth,Estimation")
f.close()

hash_count = 45
group_count = 9
cities_per_group = int(hash_count / group_count)

hashes = [[421, 167, 1610612741], [421, 397, 1610612741], [421, 257, 3145739], [479, 193, 201326611],
                        [659, 193, 100663319], [421, 193, 201326611], [619, 257, 100663319], [619, 139, 1610612741],
                        [389, 397, 100663319], [659, 193, 402653189], [479, 167, 1610612741], [479, 211, 201326611],
                        [983, 139, 201326611], [983, 257, 3145739], [443, 137, 402653189], [929, 397, 3145739],
                        [167, 431, 402653189], [421, 139, 12582917], [761, 131, 3145739], [761, 389, 402653189],
                        [317, 193, 1572869], [241, 139, 393241], [467, 167, 805306457], [109, 167, 786433],
                        [547, 397, 25165843], [109, 191, 12582917], [641, 397, 1610612741], [983, 389, 196613],
                        [641, 373, 402653189], [127, 233, 201326611], [641, 373, 25165843], [389, 107, 786433],
                        [257, 107, 1572869], [491, 149, 201326611], [389, 163, 805306457], [109, 193, 1572869],
                        [967, 277, 25165843], [953, 151, 805306457], [547, 139, 196613], [919, 419, 100663319],
                        [937, 431, 201326611], [661, 383, 1610612741], [109, 107, 6291469], [491, 293, 805306457],
                        [167, 389, 50331653]]

m = 600


def get_group_prediction(distinct_prediction):
    all_group_avg = []
    for i in range(0, group_count):
        group_avg = 0
        for j in range(0, cities_per_group):
            k = i * cities_per_group + j
            group_avg += distinct_prediction[k]
        group_avg = round(group_avg / cities_per_group)
        all_group_avg.append(group_avg)
    all_group_avg.sort()
    return all_group_avg


def get_trailing_zero(bin_val):
    return len(bin_val) - len(bin_val.rstrip('0'))


def calculate_distinct(all_hashes_bin):
    # For each hash function, calculate distinct elements for all data present in hash binary
    distinct_prediction = []
    for i in range(0, hash_count):
        max_zeroes = -1
        data_count = len(all_hashes_bin)
        for j in range(0, data_count):
            current_hash_bin = all_hashes_bin[j][i]
            z = get_trailing_zero(current_hash_bin)
            if (z > max_zeroes):
                max_zeroes = z
        distinct_prediction.append(math.pow(2, max_zeroes))
    return distinct_prediction


def get_binary_hash(individual_city_hash):
    individual_city_hash_bin = []
    for hash_val in individual_city_hash:
        binary_hash = bin(hash_val)[2:]
        individual_city_hash_bin.append(binary_hash)
    return individual_city_hash_bin


def get_hash(city_num):
    individual_city_hash = []
    for i in hashes:
        a = i[0]
        b = i[1]
        p = i[2]
        hash_val = (((a * city_num) + b) % p) % m
        individual_city_hash.append(hash_val)
    return individual_city_hash


def flajolet_martin(time, dataRdd):
    data = dataRdd.collect()
    ground_truth_cities = set()
    all_hashes = []
    all_hashes_bin = []
    for i in data:
        city = json.loads(i)["city"].encode('utf-8')
        ground_truth_cities.add(city)
        city_num = int(binascii.hexlify(city), 16)
        individual_city_hash = get_hash(city_num)
        all_hashes.append(individual_city_hash)
        individual_city_hash_bin = get_binary_hash(individual_city_hash)
        all_hashes_bin.append(individual_city_hash_bin)

    distinct_prediction = calculate_distinct(all_hashes_bin)
    group_prediction = get_group_prediction(distinct_prediction)
    # Get median distinct value
    final_distinct = group_prediction[int(group_count / 2)]

    f = open(output_file_path, "a")
    f.write("\n" + str(time) + "," + str(len(ground_truth_cities)) + "," + str(final_distinct))
    f.close()


sc = SparkContext("local[*]", "Flajolet_Martin")
ssc = StreamingContext(sc, 5)
current_stream = ssc.socketTextStream("localhost", port_number).window(30, 10)
current_stream.foreachRDD(flajolet_martin)

ssc.start()
ssc.awaitTermination()
f.close()
