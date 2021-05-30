import json

with open('Eurovision3.json') as f:
    filedata = ("[" + 
        f.read().replace("}\n\n{", "},\n{") + 
    "]")
    
with open('Eurovision3_fixed.json', 'w') as f:
    f.write(filedata)
