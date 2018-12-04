#!/usr/bin/python3

import hashlib
from hmac import HMAC
from hkdf import Hkdf
import os
import sys
import subprocess

def py_version():
  return " ".join(sys.version.split()[0:1])

def current_rev():
  result = subprocess.run(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE)
  return result.stdout.decode("utf-8").strip()

def version():
  return "%s, Python %s, commit %s" % (os.path.basename(__file__), py_version(), current_rev())

def show_version():
  print("// Generated from " + version())

def hmac(key, data):
  h = HMAC(key=key, digestmod=hashlib.blake2b)
  h.update(data)
  return h.hexdigest()

def expand(ikm, length, salt, info):
  return Hkdf(salt, ikm, hash=hashlib.blake2b).expand(info, length).hex()

def derive_key(orig_key, index, data):
  salt = data + index.to_bytes(4, byteorder="big")
  return expand(orig_key, len(orig_key), salt, b"zksync")

def config_derive_key(root, type, index, tweak):
  modifier = ((type & 0xFFFF) << 16) | (index & 0xFFFF)
  return derive_key(root, modifier, tweak)

def make_hmac_examples():
  vectors = [
    [
      "",
      ""
    ],
    [
      "00",
      ""
    ],
    [
      "",
      "00"
    ],
    [
      "0001020304050607",
      "1011121314151617"
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f",
      "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"
    ]
  ]


  print("// Test vectors for CryptoSupport.authenticate (HMAC), used in CryptoSupportTest.blake2bHmacTestVectors()")
  show_version()
  print("return new byte[][][] {")
  for v in vectors:
    result = hmac(bytearray.fromhex(v[0]), bytearray.fromhex(v[1]))
    print("\t{\n\t\tUtil.hexToBytes(\"%s\"),\n\t\tUtil.hexToBytes(\"%s\"),\n\t\tUtil.hexToBytes(\"%s\")\n\t}," % (v[1], v[0], result))
  print("};\n\n")

def make_expand_examples():
  # ikm, salt, info
  vectors = [
    [
      "",
      "",
      ""
    ],

    [
      "00",
      "",
      ""
    ],
    [
      "",
      "00",
      ""
    ],
    [
      "",
      "",
      "00"
    ],

    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
      "",
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
      "000102030405060708090a0b0c0d0e0f",
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
      "",
      "000102030405060708090a0b0c0d0e0f"
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
      "000102030405060708090a0b0c0d0e0f",
      "101112131415161718191a1b1c1d1e1f"
    ],

    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f",
      "",
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f",
      "000102030405060708090a0b0c0d0e0f",
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f",
      "",
      "000102030405060708090a0b0c0d0e0f"
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f",
      "000102030405060708090a0b0c0d0e0f",
      "101112131415161718191a1b1c1d1e1f"
    ],
  ]

  print("// Test vectors for CryptoSupport.expand (HKDF), used in CryptoSupportTest.blake2HkdfTestVectors()")
  show_version()
  print("return new byte[][][] {")
  for v in vectors:
    for length in [16, 32, 64, 128]:
      result = expand(bytearray.fromhex(v[0]), length, bytearray.fromhex(v[1]), bytearray.fromhex(v[2]))
      print("\t{\n\t\tUtil.hexToBytes(\"%s\"),\n\t\tUtil.hexToBytes(\"%s\"),\n\t\tUtil.hexToBytes(\"%s\"),\n\t\tUtil.hexToBytes(\"%s\")\n\t}," % (v[0], v[1], v[2], result))
  print("};\n\n")

def make_derive_key_examples():
  # root key, index, data
  vectors = [
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      1,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0xffffffff,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f",
      0,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f",
      0,
      "00"
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0xffffffff,
      "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
    ]
  ]

  print("// Test vectors for Key.derive, used in KeyTest.testDerive()")
  show_version()
  for v in vectors:
    result = derive_key(bytearray.fromhex(v[0]), v[1], bytearray.fromhex(v[2]))
    print("new KeyDerivationExample(\n\t\"%s\",\n\t0x%08x,\n\t\"%s\",\n\t\"%s\").validate();" % (v[0], v[1], v[2], result))
  print("\n")

def make_archive_derive_key_examples():
  vectors = [
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0,
      0,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      1,
      0,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      2,
      0,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0,
      1,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0,
      0xffff,
      ""
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0,
      0,
      "10111213"
    ],
    [
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
      0x5555,
      0x8888,
      "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
    ]
  ]

  print("// Test vectors for archive key derivation, used in ZKArchiveConfigTest.testDeriveKeyForArchiveRootMatchesTestVectors and ArchiveAccessorTest.testDeriveKeyMatchesTestVectors")
  show_version()
  for v in vectors:
    result = config_derive_key(bytearray.fromhex(v[0]), v[1], v[2], bytearray.fromhex(v[3]))
    print("new ArchiveKeyDerivationExample(\n\t\"%s\",\n\t0x%04x,\n\t0x%04x,\n\t\"%s\",\n\t\"%s\").validate();" % (v[0], v[1], v[2], v[3], result))

make_hmac_examples()
make_expand_examples()
make_derive_key_examples()
make_archive_derive_key_examples()
