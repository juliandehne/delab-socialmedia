import hashlib
import math


def convert_to_number(s):
    return int.from_bytes(s.encode(), 'little')


def convert_from_number(n):
    return n.to_bytes(math.ceil(n.bit_length() / 8), 'little').decode()


def convert_to_hash(s):
    return int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10 ** 8
