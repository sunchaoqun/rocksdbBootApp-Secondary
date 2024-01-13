import os

path='/efs/rocks-db/first-db/000004.log' # rocksdb path
if os.access(path, os.W_OK):
   print(f'Refresh path cache:{path}')
else:
   print(f'Refresh failed')