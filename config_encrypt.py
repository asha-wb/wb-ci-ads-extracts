import boto3
from Crypto import Random
from Crypto.Cipher import AES

key_id = 'xxx'
file_name = '../config_file'
key_file_name = file_name+".key"


def pad(s):
    print b"\0" * (AES.block_size - len(s) % AES.block_size)
    return s + b"\0" * (AES.block_size - len(s) % AES.block_size)


def encrypt(plaintext, plaintext_data_key, key_size=256):
    message = pad(plaintext)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(plaintext_data_key, AES.MODE_CBC, iv)
    return iv + cipher.encrypt(message)


def encrypt_config_file(file_name, plaintext_data_key):
    with open(file_name, 'rb') as infile:
        plaintext = infile.read()
    enc = encrypt(plaintext, plaintext_data_key)
    with open(file_name, 'wb') as outfile:
        outfile.write(enc)


def get_kms_data_key(key_id, file_name):
    kms = boto3.client('kms')
    data_key = kms.generate_data_key(KeyId=key_id, NumberOfBytes=32)
    plaintext_data_key = data_key['Plaintext']
    ciphertext_data_key = data_key['CiphertextBlob']
    with open(key_file_name, 'wb') as c:
        c.write(ciphertext_data_key)
    encrypt_config_file(file_name, plaintext_data_key)

get_kms_data_key(key_id, file_name)