#!/usr/bin/env python3
"""Find 'OK' and show surrounding context"""

with open('/Users/johnno/src/blustream/Z_MCU_C88CS_decrypted.bin', 'rb') as f:
    data = f.read()

# Find "OK"
target = b'OK'
offset = data.find(target)

if offset != -1:
    print(f"Found 'OK' at offset 0x{offset:08x}")
    
    # Show hex dump of surrounding context
    start = max(0, offset - 64)
    end = min(len(data), offset + 64)
    
    print(f"\nContext (0x{start:08x} to 0x{end:08x}):")
    for i in range(start, end, 16):
        hex_part = ' '.join(f'{b:02x}' for b in data[i:i+16])
        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in data[i:i+16])
        marker = '  <-- HERE' if i <= offset < i+16 else ''
        print(f'{i:08x}: {hex_part:48s} {ascii_part}{marker}')
    
    # Look for other response strings nearby
    print("\n\nSearching nearby region for other strings...")
    region_start = max(0, offset - 512)
    region_end = min(len(data), offset + 512)
    region = data[region_start:region_end]
    
    # Find printable sequences
    current_string = []
    strings_found = []
    
    for i, b in enumerate(region):
        if 32 <= b < 127:  # printable ASCII
            current_string.append(chr(b))
        else:
            if len(current_string) >= 2:
                s = ''.join(current_string)
                strings_found.append((region_start + i - len(current_string), s))
            current_string = []
    
    print(f"\nPrintable strings in nearby region:")
    for addr, s in strings_found:
        if len(s) >= 2 and not all(c in '0123456789' for c in s):
            print(f"  0x{addr:08x}: '{s}'")
