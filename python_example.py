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
tl-alp-cc-provider-secured-batch-onramp/AON/clinical_ClinicalData_s3s2_20221118034423_0/s3s2_manifest.json
"""

bucket = 'tl-alp-cc-provider-secured-batch-onramp'
file = 'clinical_ClinicalData_s3s2_20221118034423_0/s3s2_manifest.json'
directory = "~/Desktop2/s3s2-save"
org = "aon"
region = "us-west-2"
awsProfile = "tempusdevops-nishant-sharma"
pubKey = ""
privKey = ""
ssmPubKey = "/staging/n_composer/dag/sftp_to_s3_/PRIVATE_KEY_S3S2"
ssmPrivKey = "/staging/n_composer/dag/sftp_to_s3_/PUBLIC_KEY_S3S2"
filePatterns = '*/SalesDB2014_Split2*'

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
