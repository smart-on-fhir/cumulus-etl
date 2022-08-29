#!/usr/bin/env python

import sys
import csv
import db_config
import cx_Oracle

# @Author Tim Miller, PhD
# Boston Children's Hospital

def main(args):
    if len(args) < 1:
        sys.stderr.write('Required arg(s): <output filename>\n')
        sys.exit(-1)

    pt_num = args[0]
    out_file = args[1]

    con = cx_Oracle.connect(db_config.user, db_config.pw, db_config.dsn)
    cur = con.cursor()

    with open(out_file, 'wt') as of:
        writer = csv.writer(of)
        # to put a variable into a query use the syntax :var as in :pt_num below then assign
        # it to a python variable in the following arguments
        cur.execute(
            """select patient_num, instance_num, start_date, observation_blob from observation_fact where patient_num=:pt_num and sourcesystem_cd='NOTES' and rownum < 100""",
            pt_num=pt_num)

        writer.writerow(['PATIENT_NUM', 'INSTANCE_NUM', 'START_DATE', 'OBSERVATION_BLOB'])
        for row in cur:
            writer.writerow([row[0], row[1], row[2], row[3]])


if __name__ == '__main__':
    main(sys.argv[1:])