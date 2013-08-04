import json

from dmedia.drives import get_parentdir_info
from dmedia.service import get_proxy


Dmedia = get_proxy()
for (parentdir, extra) in json.loads(Dmedia.Stores()).items():
    info = get_parentdir_info(parentdir)
    print()
    print(parentdir)
    print(json.dumps(info, sort_keys=True, indent=4))

