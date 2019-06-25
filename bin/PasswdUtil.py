#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/12 15:39
# @Author  : IWC
# @Param   : 加密&解密
# @File    : PasswdUtil.py

import base64
import sys,os
from Crypto import Random
from Crypto.Cipher import AES
import LogUtil

name = os.path.basename(__file__)
log = LogUtil.Logger(name)

# 加密函数
def encrypt(originalPassword):
    bs = AES.block_size
    pad = lambda s: s + (bs - len(s) % bs) * chr(bs - len(s) % bs)
    paddPassword = pad(originalPassword)
    iv = Random.OSRNG.new().read(bs)
    key = os.urandom(32)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encryptPassword = base64.b64encode(iv + cipher.encrypt(paddPassword) + key)
    log.info('Password encryption success')
    return encryptPassword

# 解密函数
def decrypt(encryptPassword):
    base64Decoded = base64.b64decode(encryptPassword)
    bs = AES.block_size
    unpad = lambda s: s[0:-s[-1]]
    iv = base64Decoded[:bs]
    key = base64Decoded[-32:]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    originalPassword = unpad(cipher.decrypt(base64Decoded[:-32]))[bs:]
    log.info('Password decryption success')
    return originalPassword

if __name__ == '__main__':
    if len(sys.argv) == 3:
        if sys.argv[1] == '-e':
            print (encrypt(sys.argv[2]))
        elif sys.argv[1] == '-d':
            print (decrypt(sys.argv[2]))
        else:
            log.error('Parameter error, please check and try again')
    else:
        log.error('Incorrect parameter length, please check and try again')

