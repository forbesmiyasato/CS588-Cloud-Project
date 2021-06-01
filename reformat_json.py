import json
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--file', required=True, type=str,
                    help='The Malformatted JSON file')

args = parser.parse_args()
file = args.file

with open(file) as f:
    filedata = ("[" + 
        f.read().replace("}\n\n{", "},\n{") + 
    "]")
    
with open(file, 'w') as f:
    f.write(filedata)
