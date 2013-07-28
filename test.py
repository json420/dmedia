from dmedia.drives import parse_mounts, get_device, get_partition_info, get_homedir_info
from filestore import _dumps
import os

mounts = parse_mounts()
#print(_dumps(mounts))

#print(_dumps(get_partition_info(get_device(mounts['/']))))

home = os.environ['HOME']
print(home)

print(_dumps(get_homedir_info(home)))
