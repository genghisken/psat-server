# Note that this code relies on the installation of:
#  * ImageMagick (http://www.imagemagick.org/script/index.php)
#  * /usr/local/swtools/bin/fits2jpeg (ftp://ftp.cv.nrao.edu/NRAO-staff/bcotton/software/FITS2jpeg.tar.gz)

from astropy.io import fits as pf
from numpy import *
import os, sys, subprocess
from gkutils.commonutils import coords_dec_to_sex

# 2013-08-08 KWS Moved the import of Image to methods that require it. Means that we can
#                use other code without satifying the prerequisite that Image must be installed.

tempUncompressedFitsLocation = '/tmp/uncompressedfits'
tempJpegLocation = '/tmp/tempjpegs'

# NOTE: All temporary filenames MUST be preceded by their process ID.  This
#       allows multiple processes to be run in parallel without potential
#       file conflicts.

def fitsToJpegExtension(filename):
   """fitsToJpegExtension.

   Args:
       filename:
   """
   newName = filename[:filename.find('.fits')] + '.jpeg'
   return (newName)

# Haven't implemented the thickness parameter yet
# 2014-06-26 KWS Default imagemagic quality is 92. Allow it to be overridden
# 2015-03-18 KWS Added number of decimal places for standard stars RA and Dec
def addJpegCrossHairs(inputFilename, outputFilename, xhLength = 20, xhOpening = 16, xhColor = 'green1', borderColor = 'white', thickness = 1, flip = True, negate = False, objectInfo = {}, standardStars = [], pixelScale = 0.25, circleSize = 20.0, quality = 92, decimalPlacesRA = 2, decimalPlacesDec = 1, rotate = 0, finder = False):
   """addJpegCrossHairs.

   Args:
       inputFilename:
       outputFilename:
       xhLength:
       xhOpening:
       xhColor:
       borderColor:
       thickness:
       flip:
       negate:
       objectInfo:
       standardStars:
       pixelScale:
       circleSize:
       quality:
       decimalPlacesRA:
       decimalPlacesDec:
       rotate:
       finder:
   """
   from PIL import Image
   from gkutils.commonutils import coords_dec_to_sex
   im = Image.open(inputFilename)
   x = im.size[0]
   y = im.size[1]
   # 2017-09-29 KWS Is the image size odd or even.  If odd, add 1 to x/2 or y/2.
   xmid = x / 2 + x % 2
   ymid = y / 2 + y % 2
   cmd = '/usr/bin/convert -quality %d -geometry %dx%d+0+0 -bordercolor %s -border 1x1 %s -draw "stroke %s line %d %d %d %d" -draw "stroke %s line %d %d %d %d" -draw "stroke %s line %d %d %d %d" -draw "stroke %s line %d %d %d %d" ' % (
       quality,
       x, y,
       borderColor,
       inputFilename,
       xhColor,
       xmid, ymid+xhOpening/2, xmid, ymid+xhOpening/2+xhLength,
       xhColor,
       xmid, ymid-xhOpening/2, xmid, ymid-xhOpening/2-xhLength,
       xhColor,
       xmid+xhOpening/2, ymid, xmid+xhOpening/2+xhLength, ymid,
       xhColor,
       xmid-xhOpening/2, ymid, xmid-xhOpening/2-xhLength, ymid)

   if finder and not negate:
       # For ATLAS PS1 colour finders
       markerWidth = 60 / pixelScale
       markerText = '1 arcmin'

       cmd += ' -pointsize 30 -strokewidth 6 -draw "stroke red line %s %s %s %s" -strokewidth 0 -draw "stroke black fill black text %s %s \'%s\' " ' % (
              x - 380,
              y - 100,
              x - 380 + markerWidth,
              y - 100,
              x - 380,
              y - 50,
              markerText)

   # Negate only works with convert, NOT with montage
   if negate:
       # Add a 1 arcmin marker for the time being.  We can get smarter later
       markerWidth = 60 / pixelScale
       markerText = '1 arcmin'

       ps1Note = ''
       if objectInfo:
           ps1Note = 'PS1 %s band' % objectInfo['filter']

       cmd += ' -negate -pointsize 30 -strokewidth 6 -draw "stroke red line %s %s %s %s" -strokewidth 0 -draw "stroke black fill black text %s %s \'%s\' " -strokewidth 0 -draw "stroke black fill black text %s %s \'%s\' " ' % (
              x - 280,
              y - 100,
              x - 280 + markerWidth,
              y - 100,
              x - 280,
              y - 50,
              markerText,
              100,
              y - 50,
              ps1Note)

       # Add the orientiation legend
       compassWidth = 100
       cmd += ' -pointsize 30 -strokewidth 6 -draw "stroke red line %s %s %s %s stroke red line %s %s %s %s " -strokewidth 0 -draw "stroke black fill black text %s %s \'N\' text %s %s \'E\'" ' % (
              x - 150 + compassWidth,
              50,
              x - 150 + compassWidth,
              50 + compassWidth,
              x - 150,
              50 + compassWidth,
              x - 150 + compassWidth,
              50 + compassWidth,

              x - 150 + compassWidth - 10,
              50 - 10,
              x - 150 - 30,
              50 + compassWidth + 10
              )

   # Add the object and standard star labels to the image
   xIndent = 50
   yLabelPosition = 50
   if objectInfo:
      coords = coords_dec_to_sex(objectInfo['ra'], objectInfo['dec'], decimalPlacesRA = decimalPlacesRA, decimalPlacesDec = decimalPlacesDec)
      coordsString = "+: %s %s = %s" % (coords[0], coords[1], objectInfo['name'])
      cmd += ' -pointsize 30 -draw "stroke blue fill blue text %s %s \'%s\' " ' % (
          xIndent,
          yLabelPosition,
          coordsString
          )

   if standardStars:
       # Superimpose standard star markers on the image
       i = 1
       for star in standardStars:
          cmd += ' -pointsize 30 -draw "fill none stroke red circle %s %s %s %s stroke black fill black text %s %s \'%d\' " ' % (
              str(int(float(star['X_PSF'])+0.5)),
              str(int(y - float(star['Y_PSF'])+0.5)),
              str(int(float(star['X_PSF']) + circleSize+0.5)),
              str(int(y - float(star['Y_PSF'])+0.5)),
              str(int(float(star['X_PSF']) + circleSize+2.0+0.5)),
              str(int(y - float(star['Y_PSF'])+0.5)),
              i)
          i += 1

       i = 1
       for star in standardStars:
          yLabelPosition += 40
          # 2015-03-18 KWS Added number of decimal places for standard stars RA and Dec
          coords = coords_dec_to_sex(float(star['RA_J2000']), float(star['DEC_J2000']), decimalPlacesRA = decimalPlacesRA, decimalPlacesDec = decimalPlacesDec)
          coordsString = "%d: %s %s  %5.2f (%s) [%.2f\\\" E, %.2f\\\" N]" % (i, coords[0], coords[1], float(star['CAL_PSF_MAG']), star['filter'], star['offset']['E'], star['offset']['N'])
          cmd += ' -pointsize 24 -draw "stroke blue fill blue text %s %s \'%s\' " ' % (
              xIndent,
              yLabelPosition,
              coordsString
              )
          i += 1


   # Experiment to add single pixels (representing mask NaNs)
   #cmd += ' -draw "fill red1 point 150 10 point 150 11 point 150 9" ' 

   # Added -flop to switch West/East (i.e. align image with Sloan)
   if flip:
       cmd += ' -flop '

   if rotate:
       angle = rotate * 90
       cmd += ' -rotate %s ' % str(angle)

   cmd += outputFilename

   print(cmd)

   os.system(cmd)


# imageSet is a list of our renamed image filenames WITHOUT the path and the suffix.
# 2010-03-15 KWS Added ippIdet
def montageLikeImagesTogether(imageSet, PSSImageRootLocation):
   """montageLikeImagesTogether.

   Args:
       imageSet:
       PSSImageRootLocation:
   """
   from PIL import Image

   (id, mjd, diffid, ippIdet, type) = imageSet[0].split('_')
   uniqueObjectName = id + '_' +  mjd + '_' +  diffid + '_' + ippIdet + '.jpeg'
   montageLocation = PSSImageRootLocation + '/%d' % int(eval(mjd)) + '/' + 'montage/'

   if not os.path.exists(montageLocation):
      os.makedirs(montageLocation)
      os.chmod(montageLocation, 0o775)

   filenameList = ''
   montage_x = 0
   montage_y = 0
   for filename in imageSet:
      # First set the montage dimensions - defined by the largest image
      try:
         im = Image.open(PSSImageRootLocation + '/%d' % int(eval(mjd)) + '/' + filename + '.jpeg')
         x = im.size[0]
         y = im.size[1]
         if (x > montage_x):
            montage_x = x
         if (y > montage_y):
            montage_y = y

         # Add the filename to the montage command
         filenameList += PSSImageRootLocation + '/%d' % int(eval(mjd)) + '/' + filename + '.jpeg '

      except IOError as e:
         print("Image %s does not exist" % filename)

   cmd = '/usr/bin/montage -geometry %dx%d+0+0 -bordercolor gray -border 4x4 %s %s' % (montage_x, montage_y, filenameList, montageLocation + uniqueObjectName)
   os.system(cmd)
   return montageLocation + uniqueObjectName

# Might as well use this function to do standard deviation, etc as well as core NaN analysis
def getFITSImageStats (image, imageCoreSizePercent, excludeZeros = False):
   """getFITSImageStats.

   Args:
       image:
       imageCoreSizePercent:
       excludeZeros:
   """

   xsize = image.shape[0]
   corexpixels = int(xsize * imageCoreSizePercent/100.0)
   ysize = image.shape[1]
   coreypixels = int(ysize * imageCoreSizePercent/100.0)

   # Flatten the 2-D array so we can do some stats
   flatArray = image.flatten()

   # Create a mask from all the finite elements.  This is not the same as "not NaN" because it also
   # excludes +/- infinity.

   finiteMask = isfinite(flatArray)

   # Pick out only the finite elements from the flatArray into a new data array
   data = flatArray[finiteMask]

   if excludeZeros:
      data = data[data != 0]
      print(data)

   # Do the stats
   dataStdDev = std(data)
   dataMedian = median(data)

   # Where are the IPP mask pixels?
   notFiniteMask = invert(finiteMask)

   notFiniteData = flatArray[notFiniteMask]
   numberOfBadPixels = len(notFiniteData)

   maskedPixelRatio = 1.0 * numberOfBadPixels/len(flatArray)

   # Count the sub image NaNs.

   subImage = image[int(xsize/2 - corexpixels/2):int(xsize/2 + corexpixels/2), int(ysize/2 - coreypixels/2):int(ysize/2 + coreypixels/2)]

   flatArray = subImage.flatten()
   finiteMask = isfinite(flatArray)
   notFiniteMask = invert(finiteMask)

   subImageNotFiniteData = flatArray[notFiniteMask]
   subImageNumberOfBadPixels = len(subImageNotFiniteData)

   maskedPixelRatioAtCore = 1.0 * subImageNumberOfBadPixels/len(flatArray)


   return dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore


# 2014-11-18 KWS Added new getFITSImagesStats version to give me stddev, median from specified core image.

def getFITSImageStats2 (image, imageCoreSizePercent, excludeZeros = False, magicNumber = 0):
   """getFITSImageStats2.

   Args:
       image:
       imageCoreSizePercent:
       excludeZeros:
       magicNumber:
   """

   xsize = image.shape[0]
   corexpixels = int(xsize * imageCoreSizePercent/100.0)
   ysize = image.shape[1]
   coreypixels = int(ysize * imageCoreSizePercent/100.0)

   # Flatten the 2-D array so we can do some stats
   flatArray = image.flatten()

   # Create a mask from all the finite elements.  This is not the same as "not NaN" because it also
   # excludes +/- infinity.

   finiteMask = isfinite(flatArray)

   # Pick out only the finite elements from the flatArray into a new data array
   data = flatArray[finiteMask]

   if excludeZeros:
      data = data[data != magicNumber]
      print(data)

   # Do the stats
   dataStdDev = std(data)
   dataMedian = median(data)

   # Where are the IPP mask pixels?
   notFiniteMask = invert(finiteMask)

   notFiniteData = flatArray[notFiniteMask]
   numberOfBadPixels = len(notFiniteData)

   maskedPixelRatio = 1.0 * numberOfBadPixels/len(flatArray)

   # Count the sub image NaNs.

   subImage = image[int(xsize/2 - corexpixels/2):int(xsize/2 + corexpixels/2), int(ysize/2 - coreypixels/2):int(ysize/2 + coreypixels/2)]

   flatArray = subImage.flatten()
   finiteMask = isfinite(flatArray)

   coreData = flatArray[finiteMask]
   coreStdDev = std(coreData)
   coreMedian = median(coreData)

   notFiniteMask = invert(finiteMask)

   subImageNotFiniteData = flatArray[notFiniteMask]
   subImageNumberOfBadPixels = len(subImageNotFiniteData)

   maskedPixelRatioAtCore = 1.0 * subImageNumberOfBadPixels/len(flatArray)


   return dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore, coreStdDev, coreMedian

# Note that when using this utility to convert postage stamp images, the jpegFilename will be
# be a temporary file (absolute path).  Likewise this will need to open a temporary uncompressed
# FITS file.
def fitsToJpeg (fitsFilename, jpegFilename, levels = (), nsigma = 10):
   """fitsToJpeg.

   Args:
       fitsFilename:
       jpegFilename:
       levels:
       nsigma:
   """

   # 2012-01-25 KWS Set the Blank Value for the /usr/local/swtools/bin/fits2jpeg binary

   fblank = 1.234567e25

   print("Processing %s" % fitsFilename)
   baseFitsFilename = os.path.basename(fitsFilename)

   h = pf.open(fitsFilename)

   # Fpacked images will have their data in the first extension, not the primary.
   header = []
   image = []
   try:
      header = h[1].header
      image = h[1].data

   except IndexError as e:
      print("This looks unpacked.  Try opening it as unpacked...")
      header = h[0].header
      image = h[0].data

   # 2020-10-03 KWS I think we've found the source of our intermittent download problem.
   except RunTimeError as e:
      print("Something is wrong with the image for this object. Affected filename is %s." % fitsFilename)
      print("Consider re-requesting images for this object.")
      print(e)

   # Create flat array that contains no NaNs according to the above threshold setting
   # This is a very slow process.  Need to revisit this to speed things up.
   # data = []

   # for x in range(image.shape[0]):
   #    for y in range(image.shape[1]):
   #       if (not isnan(image[x][y])):
   #          data.append(image[x][y])

   # 2010-02-01 KWS Added new mechanism to eliminate NaNs from calculation. This is
   #                much faster than the original method of doing things.


   # flatArray = image.flatten()
   # nanMask = isnan(flatArray)
   # notNanMask = invert(nanMask)
   # data = flatArray[notNanMask]

   print("Calculating Standard deviation, etc")

   # dataStdDev = std(data)
   # dataMedian = median(data)

   if levels:
      (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore) = levels
   else:
      (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore) = getFITSImageStats(image = image, imageCoreSizePercent = 10)

   # Use -sigma to +10sigma around median for contrast
   minval = dataMedian - dataStdDev
   if levels:
      minval = dataMedian - dataStdDev/nsigma

   maxval = dataMedian + (nsigma * dataStdDev)

   # Now uncompress the FITS file to a temporary location

   print("Writing temporary uncompressed file")

   if not os.path.exists(tempUncompressedFitsLocation):
      os.makedirs(tempUncompressedFitsLocation)
      os.chmod(tempUncompressedFitsLocation, 0o775)

   # Append the PID to the temporary filename
   tempUncompressedFitsFilename = tempUncompressedFitsLocation + '/' + baseFitsFilename + '_' + '%d' % os.getpid()

   uncompresedHDU = pf.PrimaryHDU(image, header)

   # 2012-02-13 KWS Added 'ignore' keyword.  Some non-PS1 images (e.g. LSQ) contain non-standard FITS data
   uncompresedHDU.writeto(tempUncompressedFitsFilename, clobber=True, output_verify='ignore')

   # 2025-07-09 KWS Close the file.
   uncompresedHDU.close()

   # Now run the FITS to JPEG utility with the calculated max/min values
   # Note that FITS to JPEG inists on the image being uncompressed first...
   print("Converting to JPEG")

   cmd = '/usr/local/swtools/bin/fits2jpeg -fits %s -jpeg %s -min %f -max %f -nonLinear' % (tempUncompressedFitsFilename, jpegFilename, minval, maxval)

   os.system(cmd)
   os.remove(tempUncompressedFitsFilename)
   return maskedPixelRatio, maskedPixelRatioAtCore


# 2011-07-23 KWS New V3 images are NOT flipped.  Need to switch off flipping for these images
def convertFitsToJpegWithCrosshairs(fitsFilename, jpegFilename, flip = True, xhColor = 'green1', negate = False, nsigma = 10, objectInfo = {}, standardStars = [], pixelScale = 0.25):
   """convertFitsToJpegWithCrosshairs.

   Args:
       fitsFilename:
       jpegFilename:
       flip:
       xhColor:
       negate:
       nsigma:
       objectInfo:
       standardStars:
       pixelScale:
   """

   if not os.path.exists(tempJpegLocation):
      os.makedirs(tempJpegLocation)
      os.chmod(tempJpegLocation, 0o775)

   tempJpegFilename = tempJpegLocation + '/' + os.path.basename(fitsFilename) + '_' + '%d' % os.getpid() + '.jpeg'
   (maskedPixelRatio, maskedPixelRatioAtCore) = fitsToJpeg(fitsFilename, tempJpegFilename, nsigma = nsigma)

   # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, red = non-detection
   addJpegCrossHairs(tempJpegFilename, jpegFilename, xhColor = xhColor, flip = flip, negate = negate, objectInfo = objectInfo, standardStars = standardStars, pixelScale = pixelScale)
   os.remove(tempJpegFilename)
   return maskedPixelRatio, maskedPixelRatioAtCore


# 2014-06-02 KWS Completely new code to create a JPEG or PNG image from FITS.
#                It uses the code in img_scale.py for the sqrt scaling.
def fitsToImageTest(fitsFilename, outputFilename, nsigma = 10.0, imageQuality = 100, excludeZeros = True):
    """fitsToImageTest.

    Args:
        fitsFilename:
        outputFilename:
        nsigma:
        imageQuality:
        excludeZeros:
    """
    import img_scale
    from PIL import Image
    r = fitsFilename

    rFudge = 1.0

    minFudge = 0.0

    print("Loading r image...")
    r_img = pf.getdata(r)
    if len(r_img.shape) > 2:
       r_img = r_img[1]
       print(r_img.shape)

    (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore, coreStdDev, coreMedian) = getFITSImageStats2(image = r_img, imageCoreSizePercent = 10, excludeZeros = excludeZeros)
    rminval = dataMedian - dataStdDev*minFudge
    rmaxval = dataMedian + (nsigma * dataStdDev) * rFudge

    print(rminval, rmaxval, dataMedian)

    img = zeros((r_img.shape[0], r_img.shape[1], 3), dtype=float)

    print("Flipping r...")
    img[:,:,0] = clip(flipud(img_scale.sqrt(r_img, scale_min=rminval, scale_max=rmaxval)) * 255.0, 0.0, 255.0) #red
    img[:,:,1] = img[:,:,0]
    img[:,:,2] = img[:,:,0]

    print("Converting array to uint8...")
    img = img.astype(uint8)

    print("Saving image...")
    imgObject = Image.fromarray(img)
    imgObject.save(outputFilename, quality=imageQuality)



#dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore = (45222.9728089, 375.723999023, 0.206637446086, 1.04080011815e-05) # z-band
#dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore = (98266.6904279, 1362.02197266, 0.20699658335, 5.65140335648e-07) # y-band
#dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore = (37036.9863109, 249.924407959, 0.20610337434, 1.70484001254e-05) # i-band
#dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore = (24762.3509142, 141.288162231, 0.206266259087, 8.9951503424e-06) # r-band
#dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore = (22446.8065095, 128.954421997, 0.224521473072, 1.37517481674e-05) # g-band

def fitsToImage(r, g=None, b=None, outputFilename='/tmp/out.jpeg', nsigma = 10.0, rFudge = 1.0, gFudge = 1.0, bFudge = 1.0, minFudge = 0.0, imageQuality = 100, imageCoreSizePercent = 10, useCoreStats = False, excludeZeros = True, magicNumber = 0):
    """
    Generic code to convert FITS files into PNG or JPEG. If three images are provided the code will attempt to build a colour image.
    """
    import img_scale
    from PIL import Image

    if g is None:
        g = r

    if b is None:
        b = r

    print("Loading r image...")
    r_img = pf.getdata(r)
    (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore, coreStdDev, coreMedian) = getFITSImageStats2(image = r_img, imageCoreSizePercent = imageCoreSizePercent, excludeZeros = excludeZeros, magicNumber = magicNumber)
    if useCoreStats:
        rminval = coreMedian - coreStdDev*minFudge
        rmaxval = coreMedian + (nsigma * coreStdDev) * rFudge
    else:
        rminval = dataMedian - dataStdDev*minFudge
        rmaxval = dataMedian + (nsigma * dataStdDev) * rFudge

    print("Loading g image...")
    g_img = pf.getdata(g)
    (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore, coreStdDev, coreMedian) = getFITSImageStats2(image = g_img, imageCoreSizePercent = imageCoreSizePercent, excludeZeros = excludeZeros, magicNumber = magicNumber)
    if useCoreStats:
        gminval = coreMedian - coreStdDev*minFudge
        gmaxval = coreMedian + (nsigma * coreStdDev) * gFudge
    else:
        gminval = dataMedian - dataStdDev*minFudge
        gmaxval = dataMedian + (nsigma * dataStdDev) * gFudge

    print("Loading b image...")
    b_img = pf.getdata(b)
    (dataStdDev, dataMedian, maskedPixelRatio, maskedPixelRatioAtCore, coreStdDev, coreMedian) = getFITSImageStats2(image = b_img, imageCoreSizePercent = imageCoreSizePercent, excludeZeros = excludeZeros, magicNumber = magicNumber)
    if useCoreStats:
        bminval = coreMedian - coreStdDev*minFudge
        bmaxval = coreMedian + (nsigma * coreStdDev) * bFudge
    else:
        bminval = dataMedian - dataStdDev*minFudge
        bmaxval = dataMedian + (nsigma * dataStdDev) * bFudge

    print("New image being created...")
    img = zeros((b_img.shape[0], b_img.shape[1], 3), dtype=float)

    img[:,:,0] = clip(flipud(img_scale.sqrt(r_img, scale_min=rminval, scale_max=rmaxval)) * 255.0, 0.0, 255.0) #red
    img[:,:,1] = clip(flipud(img_scale.sqrt(g_img, scale_min=gminval, scale_max=gmaxval)) * 255.0, 0.0, 255.0) #green
    img[:,:,2] = clip(flipud(img_scale.sqrt(b_img, scale_min=bminval, scale_max=bmaxval)) * 255.0, 0.0, 255.0) #blue

    print("Converting array to uint8...")
    img = img.astype(uint8)

    if (b_img.shape[0] * b_img.shape[1] * 3) > 2**32:
        print("Generating Image object...")
        # size of image x size of image x 3 layers > 2^32.  We need to split into quarters.
        imgObject = Image.fromarray(img[0:b_img.shape[0]/2, 0:b_img.shape[1]/2, :])
        print("Saving image...")
        imgObject.save("01%s" % (outputFilename), quality=imageQuality)
        imgObject = Image.fromarray(img[b_img.shape[0]/2:b_img.shape[0], 0:b_img.shape[1]/2, :])
        print("Saving image...")
        imgObject.save("02%s" % (outputFilename), quality=imageQuality)
        imgObject = Image.fromarray(img[0:b_img.shape[0]/2, b_img.shape[1]/2:b_img.shape[1], :])
        print("Saving image...")
        imgObject.save("03%s" % (outputFilename), quality=imageQuality)
        imgObject = Image.fromarray(img[b_img.shape[0]/2:b_img.shape[0], b_img.shape[1]/2:b_img.shape[1], :])
        print("Saving image...")
        imgObject.save("04%s" % (outputFilename), quality=imageQuality)

    else:
        # The image is small enough to deal with in a single go...
        print("Saving image...")
        imgObject = Image.fromarray(img)
        imgObject.save(outputFilename, quality=imageQuality)

    return maskedPixelRatio, maskedPixelRatioAtCore


# 2014-06-18 KWS Use the new fitsToImage code and add crosshairs
# 2014-06-26 KWS Default imagemagic quality is 92. Allow it to be overridden
def convertFitsToJpegWithCrosshairs2(fitsFilename, jpegFilename, flip = True, xhColor = 'green1', negate = False, nsigma = 10, objectInfo = {}, standardStars = [], pixelScale = 0.25, quality = 92, magicNumber = 0, rotate = 0):
   """convertFitsToJpegWithCrosshairs2.

   Args:
       fitsFilename:
       jpegFilename:
       flip:
       xhColor:
       negate:
       nsigma:
       objectInfo:
       standardStars:
       pixelScale:
       quality:
       magicNumber:
       rotate:
   """

   (maskedPixelRatio, maskedPixelRatioAtCore) = fitsToImage(fitsFilename, g=fitsFilename, b=fitsFilename, outputFilename=jpegFilename, nsigma = nsigma, magicNumber = magicNumber)
   # 2015-03-18 KWS Added number of decimal places for standard stars RA and Dec.
   #                For the time being we will force these to one extra decimal
   #                place.
   addJpegCrossHairs(jpegFilename, jpegFilename, xhColor = xhColor, flip = flip, negate = negate, objectInfo = objectInfo, standardStars = standardStars, pixelScale = pixelScale, quality = quality, decimalPlacesRA = 3, decimalPlacesDec = 2, rotate = rotate)

   return maskedPixelRatio, maskedPixelRatioAtCore


def getFITSPostageStamp(filename, outputFilename, x, y, dx, dy):
    """
    Use WCSTools getfits to cut out a substamp
    """

    from pstamp_utils import PSTAMP_SUCCESS, PSTAMP_NOT_AVAILABLE, PSTAMP_EDGE_TOO_CLOSE, PSTAMP_NO_OVERLAP, PSTAMP_UNKNOWN_ERROR

    # Get the size of the existing data.  Are we too close to the edge?
    h = pf.open(filename)
    try:
        header = h[1].header
        image = h[1].data

    except IndexError as e:
        print("This looks unpacked.  Try opening it as unpacked...")
        header = h[0].header
        image = h[0].data

    sizey, sizex = image.shape
    h.close()

    status = PSTAMP_NOT_AVAILABLE

    # 2016-03-01 KWS Fixed a bug which means that we can't extract anything close to the edge.
    if x + dx/2 < sizex and y + dy/2 < sizey and x > dx/2 and y > dy/2:

        #getfits opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.im.fits 300 600 50 50 -o test.fits

        # 2014-07-01 KWS There's a bug in the photpipe version of getfits, so specify the one I installed.
        cmd = '/usr/local/swtools/wcstools/bin/getfits'
        p = subprocess.Popen([cmd, filename, '-o', outputFilename, str(x), str(y), str(dx), str(dy)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = p.communicate()

        if str(errors).strip() == '':
            status = PSTAMP_SUCCESS
        else:
            # Errors is not blank.  Need to fix!
            status = PSTAMP_SUCCESS
            print("OUTPUT" , output)
            print("ERRORS", errors)
            print("LEN ERRORS", len(errors))
    else:
        print("Stamp is too close to the edge. Image size (%d, %d), x + dx/2 = %d, y + dy/2 = %d" % (sizex, sizey, x + dx/2, y + dy/2))
        status = PSTAMP_EDGE_TOO_CLOSE

    return status


# 2025-07-02 KWS Grab the ccd size from the image shape. (NOTE: change to header info - means we don't have to load an array with the entire image.)
def getMonstaPostageStamp(filename, outputFilename, x, y, size, monstaCmd = '/atlas/vendor/monsta/bin/monsta', monstaScript = '/atlas/lib/monsta/subarray_ken3.pro', ccdSizex = 10560, ccdSizey = 10560):
    """
    Use monsta subarray to cut out a substamp
    """

    # Monsta has a dumb 80 character limit on the filename.  This means we are going
    # to have to create a temporary file and then rename it afterwards.  I should ask
    # John to increaase the filename limit to (e.g.) 256 (or better still malloc the
    # the filename memory dynamically).  A search through the code shows that most
    # of the character arrays in the F77 code are 80 bytes.

    from pstamp_utils import PSTAMP_SUCCESS, PSTAMP_NOT_AVAILABLE, PSTAMP_EDGE_TOO_CLOSE, PSTAMP_NO_OVERLAP, PSTAMP_UNKNOWN_ERROR
    import shutil

    rot = 0
    status = PSTAMP_NOT_AVAILABLE

    # 2016-06-14 KWS Check that the image actually exists before reading it.
    #                Old ATLAS data goes offline.
    if not os.path.exists(filename):
        return status, rot

    # Get the size of the existing data.  Are we too close to the edge?
    # 2022-06-07 KWS Issue 2: Problem with bad FITS file. Not sure what's causing
    #                the error yet, but this try/except block should mitigate
    #                the problem.
    try:
        h = pf.open(filename)
    except OSError as e:
        print("File %s does not appear to be a valid FITS file." % filename)
        print(e)
        return status, rot

    try:
        header = h[1].header
        try:
            pa = header['PA']
        except KeyError as e:
            # Very occasionally, the PA attribute is missing!
            print("Missing PA from header")
            pa = 0

    except IndexError as e:
        print("This looks unpacked.  Try opening it as unpacked...")
        header = h[0].header
        try:
            pa = header['PA']
        except KeyError as e:
            # Very occasionally, the PA attribute is missing!
            print("Missing PA from header")
            pa = 0

    # rotate the image to bring PA ~ 0
    if pa<-90 or pa>90:
       rot = 2

    sizey, sizex = ccdSizey, ccdSizex
    h.close()

    tempFilename = '/tmp/monsta/' + os.path.basename(filename) + '_' + str(os.getpid())

    # 2016-03-01 KWS Fixed a bug which means that we can't extract anything close to the edge.
    if x + size/2 < sizex and y + size/2 < sizey and x > size/2 and y > size/2:

        if not os.path.exists('/tmp/monsta'):
            os.makedirs('/tmp/monsta')

        # 2025-07-02 KWS Switch to calling the monsta script assuming sizex and sizey are different. Should yield the same result if both are 10560.
        #                NOTE: Normally x,y are the coords. Now we have to calculate them ourself. x = x - size/2, y = y - size/2 I think.
        print("MONSTA: ", monstaCmd, monstaScript, filename, tempFilename, x - size/2, x + size/2 - 1, y - size/2, y + size/2 - 1, sizex, sizey)
        p = subprocess.Popen([monstaCmd, monstaScript, filename, tempFilename, str(x - size/2), str(x + size/2 - 1), str(y - size/2), str(y + size/2 - 1), str(sizex), str(sizey)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #print("MONSTA: ", monstaCmd, monstaScript, filename, tempFilename, x, y, size, ccdSize)
        #p = subprocess.Popen([monstaCmd, monstaScript, filename, tempFilename, str(x), str(y), str(size), str(ccdSize)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = p.communicate()

        if errors.strip() == '':
            if 'The images do not overlap' in str(output):
                print(str(output))
                status = PSTAMP_NO_OVERLAP
            else:
                status = PSTAMP_SUCCESS
        else:
            if 'The images do not overlap' in str(output):
                print(str(output))
                status = PSTAMP_NO_OVERLAP
            else:
                status = PSTAMP_SUCCESS
            # Errors is not blank.  Need to fix!
            print("OUTPUT" , output)
            print("ERRORS", errors)
            print("LEN ERRORS", len(errors))


    else:
        print("Stamp is too close to the edge. Image size (%d, %d), x + dx/2 = %d, y + dy/2 = %d" % (sizex, sizey, x + size/2, y + size/2))
        status = PSTAMP_EDGE_TOO_CLOSE

    if os.path.exists(tempFilename):
        # 2025-07-09 KWS Emergency fix. The wpwarp2 tool can't read the FILTER info if it's not a string. Fix and rewrite.
        if '05r' in tempFilename:
            th = pf.open(tempFilename)
            th.verify('fix')
            th.writeto(tempFilename, overwrite=True, output_verify='ignore')
            th.close()
        shutil.move(tempFilename, outputFilename)
    else:
        if status == PSTAMP_SUCCESS:
            # There should only be success if there was a stamp produced.
            status = PSTAMP_UNKNOWN_ERROR

    return status, rot
