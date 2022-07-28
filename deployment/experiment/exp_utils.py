import math
import sys

def average_and_ci(content):
    floatingContent = [ float(x) for x in content]
    floatingContentSquared = [ x*x for x in floatingContent]

    l = float(len(floatingContent))

    runningSum = sum(map(float, floatingContent))
    runningSumSquared = sum(map(float, floatingContentSquared))
    avg=0.0
    ci=0.0

    if (l > 0):
        avg = runningSum / l
        avgSquared = runningSumSquared / l
        inner = avgSquared - (avg*avg)
        if inner > 0:
            sd = math.sqrt(avgSquared - (avg*avg))
            ci = (sd * 1.96) / math.sqrt(l)

    return (avg, ci)

content = []
with open(sys.argv[1]) as my_file:
    content = my_file.readlines()

content = [ x.strip() for x in content ]
(avg, ci) = average_and_ci( content )
print( "{},{}".format( avg, ci ) )
