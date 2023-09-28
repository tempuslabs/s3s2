import ctypes
from ctypes import Structure

class GoString(Structure):
    _fields_ = [("p", ctypes.c_char_p), ("n", ctypes.c_longlong)]

so = ctypes.cdll.LoadLibrary('./so/s3s2.so')

decrypt = so.Decrypt

decrypt.argtypes = [
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    GoString,
    ctypes.c_ubyte,
    ctypes.c_longlong,
    GoString,

]
decrypt.restype = ctypes.c_int
"""
BUCKET/PATH_TO_BATCH/somebatch/s3s2_manifest.json
"""

bucket = 'bucket'
file = 'path_to/batch/s3s2_manifest.json'
directory = "~/Desktop2/s3s2-save"
org = "someorg"
region = "us-west-2"
awsProfile = "someprofile"
pubKey = ""
privKey = ""
ssmPubKey = "/ssmpath/PRIVATE_KEY_S3S2"
ssmPrivKey = "/ssmpath/PUBLIC_KEY_S3S2"
filePatterns = '*/filename*'

try:

    ret_obj = decrypt(
        GoString(bucket.encode('utf-8'), len(bucket)),
        GoString(file.encode('utf-8'), len(file)),
        GoString(directory.encode('utf-8'), len(directory)),
        GoString(org.encode('utf-8'), len(org)),
        GoString(region.encode('utf-8'), len(region)),
        GoString(awsProfile.encode('utf-8'), len(awsProfile)),
        GoString(pubKey.encode('utf-8'), len(pubKey)),
        GoString(privKey.encode('utf-8'), len(privKey)),
        GoString(ssmPubKey.encode('utf-8'), len(ssmPubKey)),
        GoString(ssmPrivKey.encode('utf-8'), len(ssmPrivKey)),
        True,
        10,
        GoString(filePatterns.encode('utf-8'), len(filePatterns))
    )
except Exception as ex:
    raise ex
print("done execution")
