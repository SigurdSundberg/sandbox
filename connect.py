
import subprocess


def cpyp():
    arg = "cp -RTv ../GEO3000/code/data/map/ ~/uio/BSc_GIS/"
    subprocess.call(arg, shell=True)


# Argument to connect to mount point at UiO
con = input("Do you want to connect? ")
cpy2 = input("Do you want to copy local? ")
cpy = input("Do you want to copy? ")
snd = input("Do you want to send files? ")
connection_arg = "sudo -S sshfs sigurdsu@pingo.uio.no:/uio/hume/student-u75/sigurdsu/pc/Dokumenter/Bsc\-\ GIS/Data ~/uio/BSc_GIS"  #
copy_arg = "cp -v ~/uio/BSc_GIS/* /uio/hume/student-u75/sigurdsu/pc/Dokumenter/Bsc\-\ GIS/Data/"
if con == "1":
    subprocess.call(connection_arg, shell=True)
if cpy2 == "1":
    cpyp()
if cpy == "1":
    subprocess.call(copy_arg, shell=True)
if snd == "1":
    pass
