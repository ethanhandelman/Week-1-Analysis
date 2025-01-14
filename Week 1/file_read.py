import gzip

def print_line_number(filename, line_num):
    with gzip.open(filename, 'rt') as f:
        for i, line in enumerate(f, 1):
            if i == line_num:
                print(line.strip())
                break

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python script.py <filename> <line_number>")
        sys.exit(1)
    print_line_number(sys.argv[1], int(sys.argv[2]))