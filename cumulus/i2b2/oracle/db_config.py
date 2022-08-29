import getpass
import os

# @Author Tim Miller, PhD
# Boston Children's Hospital

user = 'I2B2_USER'

dsn = os.environ.get("BASTION_CONNECT_STRING", "localhost:53000/ORCL")

pw = os.environ.get("BASTION_PASSWORD")
if pw is None:
    pw = getpass.getpass("Enter password for %s: " % user)

ldpath = os.getenv('LD_LIBRARY_PATH', None)

if ldpath is None or not 'instantclient' in ldpath:
    raise Exception('LD_LIBRARY_PATH does not exist OR does not contain a path to instantclient. This environment variable must be set before calling scripts.')