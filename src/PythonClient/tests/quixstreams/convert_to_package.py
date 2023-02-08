# Convert the unit tests to use installed package variant of quixstreams rather than the source code found in src

import os
import fileinput 
result = [os.path.join(dp, f) for dp, dn, filenames in os.walk(os.getcwd()) for f in filenames if os.path.splitext(f)[1] == '.py'] 
for filepath in result:
  if 'convert_to_package.py' in filepath or 'convert_to_src.py' in filepath:
    continue
  with fileinput.FileInput(filepath, inplace=True) as file: 
    for line in file: 
      print(line.replace('from src import quixstreams', 'import quixstreams').replace('from src.quixstreams', 'from quixstreams'), end='')
