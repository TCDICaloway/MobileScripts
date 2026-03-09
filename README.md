# iOSandGPS

iOSandGPS.py

This will extract the ZRTCLLOCATIONMO table from the cache.sqlite DB in an iOS FFS extractions.  

You can also import GPS marker list for mapping.

Set your date filter  and timezone prior to parsing the data or adding your GPS marker.

If importing generic GPS CSV ensure your Column Headers are the following: Timestamp_Local, Latitude,	Longitude,	HorizontalAccuracy,	UNITS,	Speed (m/s),	Speed Accuracy (m/s),	Speed (MPH),	Speed Accuracy (MPH)

This is just a a quick tool for triage of Native iOS data without parsing the full FFS or exporting manually from the archive and converting Times, speed etc.




