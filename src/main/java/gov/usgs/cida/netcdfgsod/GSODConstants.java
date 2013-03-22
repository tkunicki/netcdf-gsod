/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.netcdfgsod;

import java.util.regex.Pattern;

/**
 *
 * @author tkunicki
 */
public class GSODConstants {
    public final static String HOSTNAME = "ftp.ncdc.noaa.gov";
    public final static String PATH_ROOT = "/pub/data/gsod/";
    public final static String FILE_ISH_CSV = "ish-history.csv";
    public final static String PATH_ISH_CSV = PATH_ROOT + FILE_ISH_CSV;

    public final static Pattern PATTERN_YEAR = Pattern.compile("[0-9]{4}");
    public final static Pattern PATTERN_prefix = Pattern.compile("([0-9]{6}-[0-9]{5})-.*");
}
