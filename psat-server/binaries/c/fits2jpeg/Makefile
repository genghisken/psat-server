#    Makefile to build fits2jpeg on most UNIX systems
#
#
# Define the compiler commands:
#--------------------------------
#
# Note an ansi c compatible compiler is required.
#CC = cc                                # Solaris
CC = gcc                               # SunOS

# CC is that name of the c compiler to use, some possibilities are suggested:
#CC  = /opt/TESTspro/SUNWspro/bin/cc   # Solaris NRAO
#CC  = /opt/SUNWspro/bin/cc            # Solaris
#CC  = /usr/lang/acc                   # Sun OS
#CC  = cc                              # Dec Alpha OSF/1 = Digital Unix
#CC  = cc                              # IBM RS/6000
#CC  = gcc                             # Linux
#CC  = gcc                             # FreeBSD
#CC  = cc                              # HP
#CC  = cc                              # Dec
#CC  = cc                              # SGI

#
# DEBUG is the debugger/optimizer flag, 
#-------------------------------------
# O is some general level of optimization
DEBUG = -O
#
# specify the include directory for jpeg, cfitsio libraries
#----------------------------------------------
INCDIR1 = -I$(INCDIR)
INCDIR2 = -I$(INCDIR)
#
# Define compiler flags:
#--------------------------
#    CFLAGS = $(DEBUG) $(INCDIR)                 # default for most systems
#    CFLAGS = $(DEBUG) $(INCDIR) -v -K PIC -Xc   # Sun Solaris
#    CFLAGS = $(DEBUG) -Aa $(INCDIR)             # HP
#CFLAGS = $(DEBUG) $(INCDIR1) $(INCDIR2) -v -K PIC -Xc
CFLAGS = $(DEBUG) $(INCDIR1) $(INCDIR2)
#
# Define linker flags:
#--------------------------
# -s automatically strips (removes debugging information) the executable
# For Solaris try:
#LDFLAGS = -s -v -K PIC -Xc -R $(OPENWINHOME)/lib -R /opt/SUNWmotif/lib
LDFLAGS = -s
#
# Define location of fitsio library (libfitsio.a)
#LIB1= /home/gorilla/aips/pgmr/bcotton/ftools/cfitsio/solaris 
LIB1=$(LIBDIR)
#
# Define location of jpeg library (libjpeg.a)
#LIB2= /home/gorilla/aips/pgmr/bcotton/FITSview/jpeg/solaris
LIB2=$(LIBDIR)
#
#------------------------------------------------------------------------
#  You shouldn't need to change anything below here.
#
TARGETS = fits2jpeg
all:  $(TARGETS)
#
# ARCHIVE is the name of the archive
ARCHIVE=FITS2jpeg0.1.tar
#
fits2jpeg:  FITS2jpeg.o jpegsubs.o
	$(CC) $(LDFLAGS) FITS2jpeg.o -o fits2jpeg  jpegsubs.o -L$(LIB1) -L$(LIB2) -lcfitsio -ljpeg -lm
 
FITS2jpeg.o: FITS2jpeg.c jpegsubs.h
		$(CC) -c $(CFLAGS) FITS2jpeg.c

jpegsubs.o: jpegsubs.c  jpegsubs.h
		$(CC) -c $(CFLAGS) jpegsubs.c

distrib:
	cd ..; rm -f $(ARCHIVE)
	cd ..; tar cvf $(ARCHIVE) FITS2jpeg/Makefile* 
	cd ..; tar uvf $(ARCHIVE) FITS2jpeg/*.c FITS2jpeg/*.h
	cd ..; tar uvf $(ARCHIVE) FITS2jpeg/README FITS2jpeg/LICENSE
	cd ..; tar uvf $(ARCHIVE) FITS2jpeg/changes

clobber:
	rm -f *.o
	rm -f $(TARGETS)
