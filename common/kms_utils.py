import logging
import boto3
from Crypto import Random
from Crypto.Cipher import AES

logger = logging.getLogger(__name__)


def pad(s):
    return s + b"\0" * (AES.block_size - len(s) % AES.block_size)


def encrypt(message, key, key_size=256):
    message = pad(message)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return iv + cipher.encrypt(message)


def decrypt(ciphertext, key):
    logger.info("Entering decrypt")
    iv = ciphertext[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    logger.info("Exiting decrypt")
    return plaintext.rstrip(b"\0")


def encrypt_file(plain_text_file_name,encrypted_file_name, key_file_name,encrypted_kms_key_id_file_name,ciphered_data_key_id_file_name,profile_name='default'):
    logger.info("Entering encrypt_file")
    logger.info("Read the Encrypted Key ID File")
    with open(encrypted_kms_key_id_file_name, 'rb') as fo:
        encrypted_kms_key_id = fo.read()
    with open (ciphered_data_key_id_file_name ,'rb') as fo:
        ciphered_kms_key_id=fo.read()
    kms = get_boto3_session(profile_name).client('kms')
    logger.info("Read the Plain Text File")
    with open(plain_text_file_name, 'rb') as fo:
        plaintext = fo.read()
    data_key_req = kms.generate_data_key(KeyId=decrypt(encrypted_kms_key_id, kms.decrypt(CiphertextBlob=ciphered_kms_key_id)['Plaintext']).decode('utf-8'), KeySpec='AES_256')
    data_key_ciphered = data_key_req['CiphertextBlob']
    data_key_plain_text = data_key_req['Plaintext']
    logger.info("Encrypting Plain Text Credential File with Data Key Plain Text")
    enc = encrypt(plaintext, data_key_plain_text)
    logger.info("Store the Encrypted Credential File")
    with open(encrypted_file_name, 'wb') as fo:
        fo.write(enc)
    logger.info("Store the Ciphered Data Key")
    with open(key_file_name, 'wb') as fo:
        fo.write(data_key_ciphered)
    logger.info("Exiting encrypt_file")


def encrypt_kms_key_id(key_id,enc_key_id_location,ciphered_data_key_id_file_name,profile_name="default"):
    kms = get_boto3_session(profile_name).client('kms')
    data_key_req = kms.generate_data_key(KeyId=key_id, KeySpec='AES_256')
    data_key_ciphered = data_key_req['CiphertextBlob']
    data_key_plain_text = data_key_req['Plaintext']
    logger.info("Encrypting Key ID with Data Key Plain Text")
    enc = encrypt(str.encode(key_id), data_key_plain_text)
    logger.info("Store the Encrypted Key ID File")
    with open(enc_key_id_location, 'wb') as fo:
        fo.write(enc)
    with open(ciphered_data_key_id_file_name, 'wb') as fo:
        fo.write(data_key_ciphered)
    logger.info("Exiting encrypt_kms_key_id")


def decrypt_file(enc_credential_file_name,dec_credential_file_name, ciphered_data_key_file,profile_name='default'):
    dec=decrypt_content(enc_credential_file_name, ciphered_data_key_file, profile_name=profile_name)
    with open(dec_credential_file_name, 'wb') as fo:
        fo.write(dec)
    logger.info("Exiting decrypt_file")


def decrypt_content(enc_credential_file_name, ciphered_data_key_file,profile_name="default"):
    logger.info("Entering decrypt_content")
    kms = get_boto3_session(profile_name).client('kms')
    logger.info("Reading the Encrypted Credential File")
    with open(enc_credential_file_name, 'rb') as fo:
        ciphertext = fo.read()
    logger.info("Reading the Ciphered Data Key File")
    with open(ciphered_data_key_file, 'rb') as fo:
        ciphered_data_key = fo.read()
    dec = decrypt(ciphertext, kms.decrypt(CiphertextBlob=ciphered_data_key)['Plaintext'])
    logger.info("Exiting decrypt_content")
    return dec


def get_boto3_session(profile_name="default"):
    return boto3.Session(profile_name=profile_name)

