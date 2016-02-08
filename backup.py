import os
import json
from uuid import uuid4

import boto
from boto.s3.key import Key


def connect_to_s3():
    c = boto.connect_s3(host=settings["S3"]["host"])
    return c.get_bucket(settings["S3"]["bucket"], validate=False)


def run(bucket):
    k = Key(bucket)
    for fn in settings["backup-files"]:
        k.key = settings["prefix"] + os.path.basename(fn) + '-' + uuid4().hex
        k.set_contents_from_filename(
            fn,
            reduced_redundancy=settings["S3"]["RR"])
        print("File {} uploaded".format(k.key))


if __name__ == "__main__":
    with open(
            os.path.join(
                 os.path.dirname(os.path.realpath(__file__)), "settings.json")
        ) as json_data_file:
        settings = json.load(json_data_file)

    bucket = connect_to_s3()
    print("Location: {}".format(bucket.get_location()))  # debug line
    run(bucket)
