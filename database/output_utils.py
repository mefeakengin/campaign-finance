import errno
import os

from database import general


OUT_DIR = general.DATA_DIR / 'out'
if not OUT_DIR.exists():
    os.makedirs(OUT_DIR)


def silent_remove(filename):
    """Remove a file and not raise exception if it does not exist."""
    try:
        os.remove(filename)
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise 


def write_list(l, filename):
    p = OUT_DIR / filename
    silent_remove(p)
    with open(p, 'w') as f:
        for i in l:
            f.write(str(i) + "\n")
