import os

# 合并且去重
POI_set = set()
if os.path.exists("data"):
    temp_files = os.listdir("data")
    for temp_file in temp_files:
        f = open(os.path.join("data", temp_file), "r", encoding='utf-8')
        line = f.readline()
        while line != "":
            POI_set.add(line)
            line = f.readline()
        f.close()
        os.remove(os.path.join("data", temp_file))

output_filename = "output.csv"
f = open(output_filename, "w", encoding='utf-8')
for one_record in POI_set:
    f.write(one_record)
f.close()