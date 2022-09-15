import unittest
import cx_Oracle
from cumulus.i2b2.oracle import db_config

class TestSQLDump(unittest.TestCase):

    def test_connect(self):
        _user = db_config.get_user()
        _pass = db_config.get_password()
        _dsn = db_config.get_data_source_name()

        # Fails on new MACs due to no available Oracle Python Driver :(
        _ld_path = db_config.get_library_path()

        cx_Oracle.init_oracle_client(lib_dir=_ld_path)
        con = cx_Oracle.connect(_user, _pass, _dsn)
        cur = con.cursor()


if __name__ == '__main__':
    unittest.main()
