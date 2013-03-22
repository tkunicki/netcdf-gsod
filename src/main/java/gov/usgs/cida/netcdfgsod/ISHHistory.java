/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.netcdfgsod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author tkunicki
 */
public class ISHHistory {

    // doesn't handle escaped quotes inside entry and maybe some other things...
    public final static Pattern PATTERN_entry = Pattern.compile("\"([^\"]*)\"");

    public final static String COL_USAF = "USAF";  // WMO ID
    public final static String COL_WBAN = "WBAN";
    public final static String COL_STATION_NAME = "STATION NAME";
    public final static String COL_CTRY = "CTRY";
    public final static String COL_FIPS = "FIPS";
    public final static String COL_STATE = "STATE";
    public final static String COL_CALL = "CALL";
    public final static String COL_LAT = "LAT";
    public final static String COL_LON = "LON";
    public final static String COL_ELEV = "ELEV(.1M)";

    private Map<String, ISHStation> stationMap;

    public ISHHistory() {
        stationMap = new LinkedHashMap<String, ISHStation>();
    }

    public List<ISHStation> getStationList() {
        return Collections.unmodifiableList(new ArrayList<ISHStation>(stationMap.values()));
    }

    public ISHStation getStation(String wmo, String wban) {
        return stationMap.get(new StringBuilder(wmo).append('-').append(wban).toString());
    }

    public ISHStation getStation(String filePrefix) {
        return stationMap.get(filePrefix);
    }

    public void parse(InputStream inputStream) throws IOException {
        BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream), 1<< 20);
        String currentLine = bufferedReader.readLine();
        List<String> currentList = splitAndTrimQuotes(currentLine);
        final int indexWMO = currentList.indexOf(COL_USAF);
        final int indexWBAN = currentList.indexOf(COL_WBAN);
        final int indexName = currentList.indexOf(COL_STATION_NAME);
        final int indexFIPS = currentList.indexOf(COL_FIPS);
        final int indexState = currentList.indexOf(COL_STATE);
        final int indexCall = currentList.indexOf(COL_CALL);
        final int indexLatitude = currentList.indexOf(COL_LAT);
        final int indexLongitude = currentList.indexOf(COL_LON);
        final int indexElevation = currentList.indexOf(COL_ELEV);
        ISHStation currentStation = null;
        while ((currentLine = bufferedReader.readLine()) != null) {
            currentList = splitAndTrimQuotes(currentLine);
            String wban = currentList.get(indexWBAN);
            String wmo = currentList.get(indexWMO);
            String name = currentList.get(indexName);
            String fips = currentList.get(indexFIPS);
            String state = currentList.get(indexState);
            String call = currentList.get(indexCall);
            String latitude = currentList.get(indexLatitude);
            String longitude = currentList.get(indexLongitude);
            String elevation = currentList.get(indexElevation);
            if ("US".equals(fips) &&
               !"".equals(latitude) &&
               !"".equals(longitude) &&
               !"".equals(elevation) )
            {
                currentStation = new ISHStation(
                        wmo,
                        wban,
                        name,
                        fips,
                        state,
                        call,
                        latitude,
                        longitude,
                        elevation);
                stationMap.put(currentStation.filePrefix, currentStation);
            }
        }
        bufferedReader.close();
    }

    private static List<String> splitAndTrimQuotes(String string) {
        Matcher m = PATTERN_entry.matcher(string);
        ArrayList<String> l = new ArrayList<String>();
        while(m.find()) {
            l.add(m.group(1));
        }
        return l;
    }

}
