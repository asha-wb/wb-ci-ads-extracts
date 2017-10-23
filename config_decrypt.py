#!/usr/bin/python
import boto3
import boto
from Crypto import Random
from Crypto.Cipher import AES

filename = '.boto.enc'
keyfile = filename + '.key'


def pad(s):
    return s + b"\0" * (AES.block_size - len(s) % AES.block_size)


def decrypt(ciphertext, key):
    iv = ciphertext[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    return plaintext.rstrip(b"\\")


##this uses the above functions and applies it to whole files.

def decrypt_file(file_name, key):
    with open(file_name, 'rb') as fo:
        ciphertext = fo.read()
    dec = decrypt(ciphertext, key)
    with open(file_name, 'wb') as fo:
        fo.write(dec)


def decrypt_data_key(cdata_file):
    kms = boto3.client('kms')
    with open(cdata_file, 'rb') as r:
        decrypt_data_key.cdata_key = r.read()
    decrypt_data_key.cdata_key = kms.decrypt(CiphertextBlob=decrypt_data_key.cdata_key)
    decrypt_data_key.pdata_key = decrypt_data_key.cdata_key['Plaintext']


def download_s3_object(inbucket_name, queryfile):
    # KEYID=boto.config.get('CREDENTIALS','aws_access_key_id')
    # KEY=boto.config.get('CREDENTIALS','aws_secret_access_key')
    ##set up S3 download buckets##
    inconn = boto.connect_s3()
    inbucket = inconn.get_bucket(inbucket_name)
    inkey = inbucket.get_key(queryfile)
    inkey.get_contents_to_filename(queryfile)


download_s3_object('awstestbed', '.boto.enc.key')
download_s3_object('awstestbed', '.boto.enc')
decrypt_data_key(keyfile)
decrypt_file(filename, decrypt_data_key.pdata_key)
