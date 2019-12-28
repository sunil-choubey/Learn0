#
#  New Code
#

print ("Hello World")

f=0
print(f"Value of f is {f}")

def printvalue():
    f=4
    y=5
    print(f"Value of f is {f}")
    print(f"Value of y is {y}")
    checkvalues(f,y)
    while ( f < y):
        f=f+1
        print(f"now f is {f}")
    checkvalues(f,y)
    
def checkvalues(a,b):
    message = str(a) + " is less than  " + str(b) if ( a < b ) else  str(a) + "  is greater than or equal to " + str(b)
    print(f"message is {message}")

print (f"Value of f is {f}")

if __name__ == "__main__":
    printvalue()