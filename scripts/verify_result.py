#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Result verification script
Verifies message sum in disk files
"""

import struct
import sys
import os
from pathlib import Path


# File format constants
FILE_MAGIC = 0x4D4B5444  # "MKTD"
FILE_VERSION = 2
HEADER_SIZE = 64
MSG_SIZE = 24

# File flags
FILE_FLAG_COMPLETE = 0x0001


def read_header(f):
    """Read file header (64 bytes, v2 format)"""
    data = f.read(HEADER_SIZE)
    if len(data) < HEADER_SIZE:
        return None

    # Parse file header (v2, 64 bytes):
    #   magic(4) + version(2) + flags(2) + date(4) + reserved1(4)
    #   + msg_count(8) + first_seq(8) + last_seq(8) + reserved2(24)
    (magic, version, flags, date, reserved1,
     msg_count, first_seq, last_seq,
     _r2_0, _r2_1, _r2_2) = struct.unpack('<IHHIIqqqqqq', data)

    return {
        'magic': magic,
        'version': version,
        'flags': flags,
        'date': date,
        'msg_count': msg_count,
        'first_seq': first_seq,
        'last_seq': last_seq,
        'complete': (flags & FILE_FLAG_COMPLETE) != 0
    }


def read_messages(f, count):
    """Read messages and calculate sum"""
    total_sum = 0.0
    kahan_c = 0.0  # Kahan summation compensation value
    messages = []

    for i in range(count):
        data = f.read(MSG_SIZE)
        if len(data) < MSG_SIZE:
            break

        seq_num, timestamp_ns, payload = struct.unpack('<qqd', data)
        messages.append({
            'seq_num': seq_num,
            'timestamp_ns': timestamp_ns,
            'payload': payload
        })

        # Kahan summation
        y = payload - kahan_c
        t = total_sum + y
        kahan_c = (t - total_sum) - y
        total_sum = t

    return messages, total_sum


def verify_file(filepath):
    """Verify single file"""
    print(f"\nVerifying file: {filepath}")
    print("-" * 50)

    if not os.path.exists(filepath):
        print(f"Error: File does not exist")
        return False

    file_size = os.path.getsize(filepath)
    print(f"File size: {file_size} bytes")

    with open(filepath, 'rb') as f:
        # Read file header
        header = read_header(f)
        if header is None:
            print("Error: Cannot read file header")
            return False

        # Verify magic number
        if header['magic'] != FILE_MAGIC:
            print(f"Error: Invalid magic number 0x{header['magic']:08X} (expected 0x{FILE_MAGIC:08X})")
            return False

        # Verify version
        if header['version'] != FILE_VERSION:
            print(f"Warning: Version mismatch {header['version']} (expected {FILE_VERSION})")

        print(f"File version: {header['version']}")
        print(f"File flags: 0x{header['flags']:04X} (complete={header['complete']})")
        print(f"Message count: {header['msg_count']}")
        print(f"Sequence range: [{header['first_seq']}, {header['last_seq']}]")

        # Calculate expected file size
        expected_size = HEADER_SIZE + header['msg_count'] * MSG_SIZE
        if file_size != expected_size:
            print(f"Warning: File size mismatch (expected {expected_size} bytes)")

        # Read messages
        messages, total_sum = read_messages(f, header['msg_count'])

        print(f"Read messages: {len(messages)} messages")
        print(f"Sum: {total_sum:.10f}")

        # Verify sequence number continuity
        seq_errors = 0
        for i, msg in enumerate(messages):
            if msg['seq_num'] != i:
                seq_errors += 1
                if seq_errors <= 5:
                    print(f"  Sequence number error: position {i}, actual {msg['seq_num']}")

        if seq_errors > 0:
            print(f"Total sequence number errors: {seq_errors}")
        else:
            print("Sequence number verification: PASSED")

        # Display sample messages
        if messages:
            print("\nMessage samples (first 5):")
            for msg in messages[:5]:
                print(f"  seq={msg['seq_num']}, ts={msg['timestamp_ns']}, payload={msg['payload']:.6f}")

            if len(messages) > 5:
                print("  ...")
                for msg in messages[-3:]:
                    print(f"  seq={msg['seq_num']}, ts={msg['timestamp_ns']}, payload={msg['payload']:.6f}")

    return True


def compare_sums(expected_sum, actual_sum, tolerance=1e-9):
    """Compare two sums"""
    diff = abs(expected_sum - actual_sum)
    if diff <= tolerance:
        print(f"\nVerification PASSED!")
        print(f"  Expected: {expected_sum:.10f}")
        print(f"  Actual:   {actual_sum:.10f}")
        print(f"  Diff:     {diff:.2e}")
        return True
    else:
        print(f"\nVerification FAILED!")
        print(f"  Expected: {expected_sum:.10f}")
        print(f"  Actual:   {actual_sum:.10f}")
        print(f"  Diff:     {diff:.2e} (exceeds tolerance {tolerance})")
        return False


def main():
    """Main function"""
    print("=" * 50)
    print("ReplaySystem Result Verification Tool")
    print("=" * 50)

    if len(sys.argv) < 2:
        print("\nUsage: python verify_result.py <file1.bin> [file2.bin] ...")
        print("       python verify_result.py <file.bin> <expected_sum>")
        print("\nExamples:")
        print("  python verify_result.py data/mktdata_20260101.bin")
        print("  python verify_result.py data/mktdata_20260101.bin 12345.678")
        sys.exit(1)

    files = []
    expected_sum = None

    for arg in sys.argv[1:]:
        if os.path.exists(arg) or '*' in arg:
            # Handle wildcards
            if '*' in arg:
                from glob import glob
                files.extend(glob(arg))
            else:
                files.append(arg)
        else:
            try:
                expected_sum = float(arg)
            except ValueError:
                print(f"Warning: Ignoring invalid parameter '{arg}'")

    if not files:
        print("Error: No valid data files found")
        sys.exit(1)

    all_passed = True

    for filepath in files:
        success = verify_file(filepath)
        if not success:
            all_passed = False

    # If expected value provided, compare
    if expected_sum is not None and len(files) == 1:
        with open(files[0], 'rb') as f:
            header = read_header(f)
            if header:
                _, actual_sum = read_messages(f, header['msg_count'])
                if not compare_sums(expected_sum, actual_sum):
                    all_passed = False

    print("\n" + "=" * 50)
    if all_passed:
        print("All verifications passed!")
        sys.exit(0)
    else:
        print("Some verifications failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()
