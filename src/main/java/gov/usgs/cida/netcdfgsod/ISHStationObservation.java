/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.netcdfgsod;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tkunicki
 */
public class ISHStationObservation {

    public final int wmo;
    public final int wban;
    public final int year;
    public final int month;
    public final int day;
    public final float temp;
    public final int tempN;
    public final float dewp;
    public final int dewN;
    public final float slp;
    public final int slpN;
    public final float stp;
    public final int stpN;
    public final float visib;
    public final int visibN;
    public final float wdsp;
    public final int wdspN;
    public final float mxspd;
    public final float gust;
    public final float max;
    public final char maxF;
    public final float min;
    public final char minF;
    public final float prcp;
    public final char prcpF;
    public final float sndp;
    public final int frshtt;

    public ISHStationObservation(String data) {
        wmo     = Integer.parseInt(data.substring(  0,   6).trim());
        wban    = Integer.parseInt(data.substring(  7,  12).trim());
        year    = Integer.parseInt(data.substring( 14,  18).trim());
        month   = Integer.parseInt(data.substring( 18,  20).trim());
        day     = Integer.parseInt(data.substring( 20,  22).trim());
        temp    = Float.parseFloat(data.substring( 24,  30).trim());
        tempN   = Integer.parseInt(data.substring( 31,  33).trim());
        dewp    = Float.parseFloat(data.substring( 36,  41).trim());
        dewN    = Integer.parseInt(data.substring( 42,  44).trim());
        slp     = Float.parseFloat(data.substring( 46,  52).trim());
        slpN    = Integer.parseInt(data.substring( 53,  55).trim());
        stp     = Float.parseFloat(data.substring( 57,  63).trim());
        stpN    = Integer.parseInt(data.substring( 64,  66).trim());
        visib   = Float.parseFloat(data.substring( 68,  73).trim());
        visibN  = Integer.parseInt(data.substring( 74,  76).trim());
        wdsp    = Float.parseFloat(data.substring( 78,  83).trim());
        wdspN   = Integer.parseInt(data.substring( 84,  86).trim());
        mxspd   = Float.parseFloat(data.substring( 88,  93).trim());
        gust    = Float.parseFloat(data.substring( 95, 100).trim());
        max     = Float.parseFloat(data.substring(102, 108).trim());
        maxF    = data.charAt(108);
        min     = Float.parseFloat(data.substring(110, 116).trim());
        minF    = data.charAt(116);
        prcp    = Float.parseFloat(data.substring(118, 123).trim());
        prcpF   = data.charAt(123);
        sndp    = Float.parseFloat(data.substring(125, 130).trim());
        frshtt  = Integer.parseInt(data.substring(132, 138).replaceAll("[^0]{1}", "1").trim(), 2);
    }

    public static List<ISHStationObservation> parse(InputStream inputStream) throws IOException {
        List<ISHStationObservation> list = new ArrayList<ISHStationObservation>();
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream));
        // read and toss header...
        String currentLine = bufferedReader.readLine();
        while( (currentLine = bufferedReader.readLine()) != null) {
            try {
                list.add(new ISHStationObservation(currentLine));
            } catch (Exception e) {
                // TODO: need some kind of warning
            }
        }
        return list;
    }

    public static void main(String[] args) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("/Users/tkunicki/Downloads/010010-99999-2010.op");
            for (Object o : parse(fis)) {
                System.out.println(o);
            }
        } catch (Exception ex) {
            Logger.getLogger(ISHStationObservation.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ex) {
                    Logger.getLogger(ISHStationObservation.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
}
