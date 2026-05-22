#!/usr/bin/env python
"""Request Pan-STARRS Postage Stamps by ID or Exposure Name. The coordsandid is a comma separated set (no spaces) of one or more triplets of RA, Dec and processing ID.
        
Usage:
  %s <coordsandid> [--imagetype=<imagetype>] [--optionmask=<optionmask>] [--imagesize=<imagesize>] [--skycell=<skycell>] [--allskycells] [--requestprefix=<requestprefix>] [--coordmask=<coordmask>] [--requesttype=<requesttype>] [--camera=<camera>] [--alias=<alias>] [--requesthome=<requesthome>] [--stampuser=<stampuser>] [--stamppass=<stamppass>] [--uploadurl=<uploadurl>] [--datastoreurl=<datastoreurl>]
  %s (-h | --help)
  %s --version  
                
Options:        
  -h --help                            Show this screen.
  --version                            Show version.
  --imagetype=<imagetype>              Image type to request (warp|stack|diff|chip|SSdiff) [default: diff]
  --optionmask=<optionmask>            Bitmap of which options to send to the stamp server. See code for meanings [default: 2049].
  --imagesize=<imagesize>              Size of each stamp edge in pixels. Pixel scale is 0.25"/pixel and default size is 5 arcmins [default: 1200].
  --skycell=<skycell>                  Ignore the size option above and request the specified full skycell [default: null].
  --allskycells                        Ignore the size option above and skycell option and request ALL the relevant skycells for that exposure.
  --requestprefix=<requestprefix>      Stamp request prefix [default: manual_pstamp_request].
  --coordmask=<coordmask>              Coordinates mask 0, 1, 2 or 3 [default: 2].
  --requesttype=<requesttype>          Type of image request (byid|byexp) [default: byid].
  --camera=<camera>                    Which Pan-STARRS detector (gpc1|gpc2) [default: gpc1].
  --alias=<alias>                      Parse an alias (name of object) in request list.
  --requesthome=<requesthome>          Place to store the FITS request before sending [default: /tmp].
  --stampuser=<stampuser>              Postage stamp service username.
  --stamppass=<stamppass>              Postage stamp service password.
  --uploadurl=<uploadurl>              The Postage stamp service URL.
  --datastoreurl=<datastoreurl>        The datastore URL from where we will download our request results.

Example:        
  python %s 192.98514,+16.28158,52765 --imagetype=warp --camera=gpc2 --stampuser=theuser --stamppass=**** --uploadurl=https://******** --downloadurl=https://******
  python %s /tmp/warps.txt --imagetype=warp --camera=gpc2 --stampuser=theuser --stamppass=**** --uploadurl=https://****** --downloadurl=https://******
"""                 
                    
import sys          
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

from pstamp_utils import sendPSRequest, writeFGSSPostageStampRequestById
from gkutils.commonutils import coords_sex_to_dec, divideList, cleanOptions, Struct
import datetime

PSTAMP_SELECT_IMAGE = 1
PSTAMP_SELECT_MASK = 2
PSTAMP_SELECT_VARIANCE = 4
PSTAMP_SELECT_CMF = 8
PSTAMP_SELECT_PSF = 16
PSTAMP_SELECT_BACKMDL = 32
PSTAMP_SELECT_INVERSE = 1024
PSTAMP_SELECT_UNCONV = 2048

#DEFAULT_OPTION_MASK = PSTAMP_SELECT_UNCONV + PSTAMP_SELECT_CMF + PSTAMP_SELECT_IMAGE
DEFAULT_OPTION_MASK = PSTAMP_SELECT_UNCONV + PSTAMP_SELECT_IMAGE
MINIMUM_IMAGE_SIZE = 50


# ###########################################################################################
#                                         Main program
# ###########################################################################################

# BYPASS the DATABASE.  If we know the ID of the request and its central coordinates, just
# make the request

def main(argv = None):
   """main.

   Args:
       argv:
   """
   opts = docopt(__doc__, version='0.1')
   opts = cleanOptions(opts)

   # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
   options = Struct(**opts)

   if options.uploadurl is None:
       print("Please specify where to send the stamp request.")
       sys.exit(0)

   if options.datastoreurl is None:
       print("Please specify the Pan-STARRS datastore URL.")
       sys.exit(0)

   if ',' in options.coordsandid:
       # We have a list of ra,dec,id(,alias)
       idList = options.coordsandid.split(',')
       if options.alias:
           if len(idList) % 4 > 0 or len(idList) < 4:
              sys.exit("The ID list must be divisible by 4")
           chunks, newlist = divideList(idList, listChunkSize = 4)
       else:
           if len(idList) % 3 > 0 or len(idList) < 3:
              sys.exit("The ID list must be divisible by 3")
           chunks, newlist = divideList(idList, listChunkSize = 3)
   else:
       # We have a filename. The list of coords and IDs are in a file.
       newlist = []
       try:
           with open(options.coordsandid, 'r') as f:
               lines = f.readlines()
               for line in lines:
                   newlist.append(line.strip().split(','))
           if len(newlist[0]) == 3 and options.alias is False:
               sys.exit("The ID list must be divisible by 3")
           if len(newlist[0]) == 3 and options.alias:
               sys.exit("The ID list must be divisible by 4")
       except FileNotFoundError as e:
           sys.exit("Cannot find file %s. Aborting." % options.coordsandid)


   print("LENGTH of list = %d" % len(newlist))

   optionMask = None

   allskycells = options.allskycells

   # If the list isn't specified assume it's the Good List.
   if options.optionmask is not None:
      try:
         optionMask = int(options.optionmask)
      except ValueError as e:
         sys.exit("Option mask must be an integer")


   imageSize = options.imagesize
   try:
      imageSize = int(options.imagesize)
   except ValueError as e:
      sys.exit("Image size must be an integer")

   if imageSize > 4800:
      imageSize = 4800

   if imageSize < MINIMUM_IMAGE_SIZE:
      imageSize = MINIMUM_IMAGE_SIZE

   print("Option mask =", optionMask, " Image size =", imageSize)


   requestPrefix = options.requestprefix


   idList = []

   requestType = options.requesttype


   # Construct a fake DB result set

   try:
      # Check the first ID value. If it is not an integer use 'byexp' to make the request
      id = int(newlist[0][2])
   except ValueError as e:
      requestType = 'byexp'

   results = []
   for triplet in newlist:
      comment = None
      id = triplet[2]
      if options.alias:
         comment = triplet[3]
      try:
         ra = float(triplet[0])
         dec = float(triplet[1])
      except ValueError:
         try:
            ra, dec = coords_sex_to_dec(triplet[0], triplet[1])
         except Exception as e:
            sys.exit("Can't parse the coordinates.")

      row = {'warp_id': id,
              'ra_psf': float(ra),
              'dec_psf': float(dec),
              'id': id,
              'tdate': '99999.999',
              'imageid': str(id),
              'ipp_idet': 0,
              'comment': comment
              }
      results.append(row)

   # Get the date.  Could use "now()" when inserting into database
   # but this ensures that the request name suffix and the date in the
   # database are always identical (nice to have consistency).
   currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")

   (year, month, day, hour, min, sec) = currentDate.split(':')

   timeReqeustSuffix = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

   requestName = "%s_%s" % (requestPrefix, timeReqeustSuffix)
   requestFileName = "%s/%s.fits" % (options.requesthome, requestName)


   if allskycells:
      # Ignore the coordinates, just get the complete list of skycells for the specified warp_id (usually around 60)
      writeFGSSPostageStampRequestById(requestFileName, requestName, results, 0, 0, psRequestType = requestType, optionMask = 2057, imageType = options.imagetype, psJobType = 'get_image', camera = options.camera)
   else:
      writeFGSSPostageStampRequestById(requestFileName, requestName, results, imageSize, imageSize, psRequestType = requestType, optionMask = optionMask, imageType = options.imagetype, skycell = options.skycell, coordMask = int(options.coordmask), camera = options.camera)

   #Send the request to the postage stamp server
   pssServerId = sendPSRequest(requestFileName, requestName, username = options.stampuser, password = options.stamppass, postageStampServerURL = options.uploadurl)

   if (pssServerId >= 0):
      print("Successfully submitted job to Postage Stamp Server (request ID = %d)." % pssServerId)
      print("Predicted URL is: %s" % options.datastoreurl + requestName)
   else:
      print("Did Not successfully submit the job to the Postage Stamp Server!")

   return

# ###########################################################################################
#                                         Main hook
# ###########################################################################################


if __name__=="__main__":
    main()

