create or replace view tcs_2mass_psc_cat as select * from panstarrs1.tcs_2mass_psc_cat;
create or replace view tcs_2mass_xsc_cat as select * from panstarrs1.tcs_2mass_xsc_cat;
create or replace view tcs_guide_star_cat as select * from panstarrs1.tcs_guide_star_cat;
create or replace view tcs_ned_cat as select * from panstarrs1.tcs_ned_cat;
create or replace view tcs_sdss_galaxies_cat as select * from panstarrs1.tcs_sdss_galaxies_cat;
create or replace view tcs_sdss_spect_galaxies_cat as select * from panstarrs1.tcs_sdss_spect_galaxies_cat;
create or replace view tcs_sdss_stars_cat as select * from panstarrs1.tcs_sdss_stars_cat;
create or replace view tcs_veron_cat as select * from panstarrs1.tcs_veron_cat;
-- New DRY Medium Deep Specific Catalogues
create or replace view tcs_cat_deep2dr3 as select * from panstarrs1.tcs_cat_deep2dr3;
create or replace view tcs_cat_md01_chiappetti2005 as select * from panstarrs1.tcs_cat_md01_chiappetti2005;
create or replace view tcs_cat_md01_ned as select * from panstarrs1.tcs_cat_md01_ned;
create or replace view tcs_cat_md01_pierre2007 as select * from panstarrs1.tcs_cat_md01_pierre2007;
create or replace view tcs_cat_md02_giacconi2002 as select * from panstarrs1.tcs_cat_md02_giacconi2002;
create or replace view tcs_cat_md02_lefevre2004 as select * from panstarrs1.tcs_cat_md02_lefevre2004;
create or replace view tcs_cat_md02_lehmer2005 as select * from panstarrs1.tcs_cat_md02_lehmer2005;
create or replace view tcs_cat_md02_ned as select * from panstarrs1.tcs_cat_md02_ned;
create or replace view tcs_cat_md02_virani2006 as select * from panstarrs1.tcs_cat_md02_virani2006;
create or replace view tcs_cat_md03_ned as select * from panstarrs1.tcs_cat_md03_ned;
create or replace view tcs_cat_md04_hasinger2007 as select * from panstarrs1.tcs_cat_md04_hasinger2007;
create or replace view tcs_cat_md04_ned as select * from panstarrs1.tcs_cat_md04_ned;
create or replace view tcs_cat_md04_trump2007 as select * from panstarrs1.tcs_cat_md04_trump2007;
create or replace view tcs_cat_md05_brunner2008 as select * from panstarrs1.tcs_cat_md05_brunner2008;
create or replace view tcs_cat_md05_ned as select * from panstarrs1.tcs_cat_md05_ned;
create or replace view tcs_cat_md06_ned as select * from panstarrs1.tcs_cat_md06_ned;
create or replace view tcs_cat_md07_laird2009 as select * from panstarrs1.tcs_cat_md07_laird2009;
create or replace view tcs_cat_md07_nandra2005 as select * from panstarrs1.tcs_cat_md07_nandra2005;
create or replace view tcs_cat_md07_ned as select * from panstarrs1.tcs_cat_md07_ned;
create or replace view tcs_cat_md08_manners2003 as select * from panstarrs1.tcs_cat_md08_manners2003;
create or replace view tcs_cat_md08_ned as select * from panstarrs1.tcs_cat_md08_ned;
create or replace view tcs_cat_md09_ned as select * from panstarrs1.tcs_cat_md09_ned;
create or replace view tcs_cat_md10_ned as select * from panstarrs1.tcs_cat_md10_ned;

-- 2010-11-10 KWS Added new views onto new tcs_cat_sdss_stars_galaxies table
create or replace view tcs_cat_sdss_stars_galaxies as select * from panstarrs1.tcs_cat_sdss_stars_galaxies;
create or replace view tcs_cat_v_sdss_starsgalaxies_galaxies as select * from panstarrs1.tcs_cat_sdss_stars_galaxies where type = 3;
create or replace view tcs_cat_v_sdss_starsgalaxies_stars as select * from panstarrs1.tcs_cat_sdss_stars_galaxies where type = 6;

-- 2011-12-12 KWS New catalogues
create or replace view tcs_cat_milliquas as select * from panstarrs1.tcs_cat_milliquas;

-- 2012-12-01 KWS New SDSS DR9 views
create or replace view tcs_cat_sdss_dr9_photo_stars_galaxies as select * from panstarrs1.tcs_cat_sdss_dr9_photo_stars_galaxies;
create or replace view tcs_cat_sdss_dr9_spect_galaxies_qsos as select * from panstarrs1.tcs_cat_sdss_dr9_spect_galaxies_qsos;
create or replace view tcs_cat_v_sdss_dr9_galaxies_notspec as select * from panstarrs1.tcs_cat_v_sdss_dr9_galaxies_notspec;
create or replace view tcs_cat_v_sdss_dr9_stars as select * from panstarrs1.tcs_cat_v_sdss_dr9_stars;
create or replace view tcs_cat_v_sdss_dr9_spect_galaxies as select * from panstarrs1.tcs_cat_v_sdss_dr9_spect_galaxies;
create or replace view tcs_cat_v_sdss_dr9_spect_qsos as select * from panstarrs1.tcs_cat_v_sdss_dr9_spect_qsos;

-- 2013-02-05 KWS New PS1 Medium Deep Reference Catalogue view
create or replace view tcs_cat_ps1_medium_deep_ref as select * from panstarrs1.tcs_cat_ps1_medium_deep_ref;
create or replace view tcs_cat_v_ps1_medium_deep_ref_stars as select * from panstarrs1.tcs_cat_v_ps1_medium_deep_ref_stars;
create or replace view tcs_cat_v_ps1_medium_deep_ref_galaxies as select * from panstarrs1.tcs_cat_v_ps1_medium_deep_ref_galaxies;

-- 2014-08-14 KWS New Kepler Galaxies Catalogue view
-- 2014-11-14 KWS Renamed the Kepler Catalog
create or replace view tcs_cat_kepler_k2 as select * from panstarrs1.tcs_cat_kepler_k2;

-- 2017-12-05 KWS New Kepler Pixels catalogue view
create or replace view tcs_cat_kepler_k2_pixels as select * from panstarrs1.tcs_cat_kepler_k2_pixels;

-- 2015-02-23 KWS Added new PS1 Ubercal Stars catalogue
create or replace view tcs_cat_ps1_ubercal_stars as select * from panstarrs1.tcs_cat_ps1_ubercal_stars;
-- 2016-09-16 KWS Added the Gaia DR1 catalogue
create or replace view tcs_cat_gaia_dr1 as select * from panstarrs1.tcs_cat_gaia_dr1;

-- CREATE A VIEW ON THE 2MASS PSC : EXCLUDE 'EXTENDED' SOURCES (gal_contam = 1) AND 'BOTH' SOURCES (gal_contam = 2)
-- THIS IS DONE BY SELECTING ONLY POINT SOURCES (gal_contam = 0)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_2mass_psc_noextended AS
SELECT * FROM tcs_2mass_psc_cat 
WHERE gal_contam = 0;

-- CREATE A VIEW ON THE GSCv2.3 : EXCLUDE 'probably not point sources' SOURCES (Class = 3)
-- THIS IS DONE BY SELECTING ONLY 'probably point source' SOURCES (Class = 0)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_guide_star_ps AS
SELECT * FROM tcs_guide_star_cat 
WHERE classification = 0;

-- CREATE A VIEW ON THE GSCv2.3 : EXCLUDE 'point sources' SOURCES (Class = 0)
-- THIS IS DONE BY SELECTING ONLY 'probably not point source' SOURCES (Class = 3)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_guide_star_notps AS
SELECT * FROM tcs_guide_star_cat 
WHERE classification = 3;

-- CREATE A VIEW ON THE SDSS GALAXIES : EXCLUDE SpectObjAll GALAXIES
-- THIS IS DONE BY SELECTING ONLY NON-SPECTROSCOPICALLY TARGETED SOURCES (SpectObjID = 0)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_sdss_galaxies_notspec AS
SELECT * FROM tcs_sdss_galaxies_cat
WHERE SpecObjID = 0;


-- CREATE A VIEW ON THE DEEP2 CATALOGUE : SELECT GALAXIES ONLY
-- THIS IS DONE BY SELECTING ONLY GALAXIES (CLASS == GALAXY)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_deep2dr3_galaxies AS
SELECT * FROM tcs_cat_deep2dr3
WHERE CLASS = "GALAXY";



-- CREATE A VIEWS ON THE MD NED CATALOGUES : SELECT GALAXIES ONLY
-- THIS IS DONE BY SELECTING ONLY GALAXIES (Type == G)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md01_ned_galaxies AS
SELECT * FROM tcs_cat_md01_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md02_ned_galaxies AS
SELECT * FROM tcs_cat_md02_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md03_ned_galaxies AS
SELECT * FROM tcs_cat_md03_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md04_ned_galaxies AS
SELECT * FROM tcs_cat_md04_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md05_ned_galaxies AS
SELECT * FROM tcs_cat_md05_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md06_ned_galaxies AS
SELECT * FROM tcs_cat_md06_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md07_ned_galaxies AS
SELECT * FROM tcs_cat_md07_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md08_ned_galaxies AS
SELECT * FROM tcs_cat_md08_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md09_ned_galaxies AS
SELECT * FROM tcs_cat_md09_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md10_ned_galaxies AS
SELECT * FROM tcs_cat_md10_ned
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";





-- CREATE A VIEWS ON THE MD NED CATALOGUES : SELECT QSOs ONLY
-- THIS IS DONE BY SELECTING ONLY QSOs (Type == QSO)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md01_ned_qsos AS
SELECT * FROM tcs_cat_md01_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md02_ned_qsos AS
SELECT * FROM tcs_cat_md02_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md03_ned_qsos AS
SELECT * FROM tcs_cat_md03_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md04_ned_qsos AS
SELECT * FROM tcs_cat_md04_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md05_ned_qsos AS
SELECT * FROM tcs_cat_md05_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md06_ned_qsos AS
SELECT * FROM tcs_cat_md06_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md07_ned_qsos AS
SELECT * FROM tcs_cat_md07_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md08_ned_qsos AS
SELECT * FROM tcs_cat_md08_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md09_ned_qsos AS
SELECT * FROM tcs_cat_md09_ned
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md10_ned_qsos AS
SELECT * FROM tcs_cat_md10_ned
WHERE Type = "QSO";





-- CREATE A VIEWS ON THE MD NED CATALOGUES : SELECT XrayS ONLY
-- THIS IS DONE BY SELECTING ONLY X-RAY SOURCES (Type == XrayS)

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md01_ned_xrays AS
SELECT * FROM tcs_cat_md01_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md02_ned_xrays AS
SELECT * FROM tcs_cat_md02_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md03_ned_xrays AS
SELECT * FROM tcs_cat_md03_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md04_ned_xrays AS
SELECT * FROM tcs_cat_md04_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md05_ned_xrays AS
SELECT * FROM tcs_cat_md05_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md06_ned_xrays AS
SELECT * FROM tcs_cat_md06_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md07_ned_xrays AS
SELECT * FROM tcs_cat_md07_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md08_ned_xrays AS
SELECT * FROM tcs_cat_md08_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md09_ned_xrays AS
SELECT * FROM tcs_cat_md09_ned
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md10_ned_xrays AS
SELECT * FROM tcs_cat_md10_ned
WHERE Type = "XrayS";



-- CREATE A VIEWS ON THE MD NED CATALOGUES : SELECT SOURCES THAT ARE NOT GALAXIES or QSOs

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md01_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md01_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md02_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md02_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md03_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md03_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md04_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md04_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md05_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md05_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md06_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md06_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md07_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md07_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md08_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md08_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md09_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md09_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_md10_ned_not_gal_qso AS
SELECT * FROM tcs_cat_md10_ned
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";



-- 2012-10-10 KWS Created generic (i.e. non-MD-specific) NED QSO, X-ray, Non-QSO/Galaxy

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_ned_qsos AS
SELECT * FROM tcs_ned_cat
WHERE Type = "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_ned_xrays AS
SELECT * FROM tcs_ned_cat
WHERE Type = "XrayS";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_ned_not_gal_qso AS
SELECT * FROM tcs_ned_cat
WHERE Type != "G" and Type != "GPair" and Type != "GTrpl" and Type != "GGroup" and Type != "QSO";

CREATE OR REPLACE ALGORITHM = MERGE VIEW tcs_cat_v_ned_galaxies AS
SELECT * FROM tcs_ned_cat
WHERE Type = "G" or Type = "GPair" or Type = "GTrpl" or Type = "GGroup";

