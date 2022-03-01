import sys

def do_work(fname):
        f = open(fname)
        for line in f:
                #sl = line.split()
                #if len(sl) > 2 and sl[1] == "usertable":
                #       print(sl[2][4:])

                print(line[4:], end="")

do_work(sys.argv[1])
