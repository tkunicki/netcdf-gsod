/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.usgs.cida.netcdfgsod;

import com.sun.jna.NativeLong;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.NativeLongByReference;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.unidata.ucar.netcdf.jna.NC.*;
import static edu.unidata.ucar.netcdf.jna.NCUtil.*;
import static java.lang.System.*;

/**
 *
 * @author tkunicki
 */
public class GSODNetCDFGenerator {

    private static final int STRING_LENGTH = 32;

    private static final short _FillValue_SHORT = -9999;

    private final static String STATION_DIM_NAME = "station";
    private final static String OBSERVATION_DIM_NAME = "observation";
    private final static String OBSERVATION_STRUCT_NAME = "record"; // NetCDF-Java reqiures this to be record (last tested release was 4.2.26)
    
    public static void main(String[] args) {
        String jnaLibraryPath = getProperty("jna.library.path");
        if (jnaLibraryPath == null) {
//            setProperty("jna.library.path", "/Users/tkunicki/Library/NetCDF/4.1/lib");
//            setProperty("jna.library.path", "/opt/local/lib/netcdf-devel/lib");
            setProperty("jna.library.path", "/opt/local/lib");
        }

        out.println(nc_inq_libvers());

        File gsodDirectory = new File("/Users/tkunicki/Data/GSOD");
        File opDirectory = new File("/Users/tkunicki/Data/GSOD/op");
        File netcdfDirectory = new File("/Users/tkunicki/Data/GSOD/netcdf");
        File inventoryFile = new File(gsodDirectory, GSODConstants.FILE_ISH_CSV);

        try {

            if (!netcdfDirectory.exists()) {
                netcdfDirectory.mkdirs();
            }
            if (!opDirectory.exists()) {
                opDirectory.mkdirs();
            }

            InputStream is = new BufferedInputStream(new FileInputStream(inventoryFile));
            ISHHistory ishhistory = new ISHHistory();
            ishhistory.parse(is);
            is.close();

            List<ISHStation> station_list = ishhistory.getStationList();

            List<File> yearList = new ArrayList<File>();
            for (File file0 : gsodDirectory.listFiles()) {
                if(GSODConstants.PATTERN_YEAR.matcher(file0.getName()).matches()) {
                    yearList.add(file0);
                    File file1 = new File(opDirectory, file0.getName());
                    if(!file1.exists()) {
                        file1.mkdir();
                    }
                }
            }
            Collections.sort(yearList);

            Map<ISHStation, List<File>> stationFileMap = new LinkedHashMap<ISHStation, List<File>>();
            out.println("Parsing op data files and unzipping if needed...");
            for(ISHStation station : station_list) {
                List<File> stationFileList = new ArrayList<File>();
                for (File year : yearList) {
                    File zippedDataFile = new File(year, station.filePrefix +  "-" + year.getName() +".op.gz");
                    if (zippedDataFile.exists()) {
                        File unzippedDataFile = new File(new File(opDirectory, year.getName()), station.filePrefix +  "-" + year.getName() +".op");
                        if ( !unzippedDataFile.exists() || zippedDataFile.lastModified() > unzippedDataFile.lastModified()) {
                            InputStream zis = null;
                            OutputStream zos = null;
                            try {
                                zis = new GZIPInputStream(new FileInputStream(zippedDataFile), 8 << 10);
                                zos = new BufferedOutputStream(new FileOutputStream(unzippedDataFile), 8 << 10);
                                byte[] buffer = new byte[8 << 10];
                                int read = 0;
                                while ( (read = zis.read(buffer)) > -1) {
                                    zos.write(buffer, 0, read);
                                }
                            } catch (IOException e) {
                            } finally {
                                if (zis != null) { try { zis.close(); zis = null; } catch (IOException e) { } }
                                if (zos != null) { try { zos.close(); zos = null; } catch (IOException e) { } }
                            }

                            unzippedDataFile.setLastModified(zippedDataFile.lastModified());
                            stationFileList.add(unzippedDataFile);
                        } else {
                            stationFileList.add(unzippedDataFile);
                        }
                    }
                }
                if (stationFileList.size() > 0) {
                    stationFileMap.put(station, stationFileList);
                }
            }

            station_list = new ArrayList<ISHStation>(stationFileMap.keySet());
            int station_count = station_list.size();

            out.println("Reading op data files and creating NetCDF file(s)...");

            List<GSODNetCDFFileBase> gsodList = new ArrayList<GSODNetCDFFileBase>();
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.c.cf.nc")), station_count, NC_CLASSIC_MODEL, Convention.CF));
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.c.uod.nc")), station_count, NC_CLASSIC_MODEL, Convention.UOD));
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.4c.cf.nc")), station_count, NC_NETCDF4 | NC_CLASSIC_MODEL, Convention.CF));
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.4c.uod.nc")), station_count, NC_NETCDF4 | NC_CLASSIC_MODEL, Convention.UOD));
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.4.cf.nc")), station_count, NC_NETCDF4, Convention.CF));
//            gsodList.add(new GSODNetCDFFile((new File(netcdfDirectory, "gsod.4.uod.nc")), station_count, NC_NETCDF4, Convention.UOD));
            gsodList.add(new GSODStructureNetCDFFile((new File(netcdfDirectory, "gsod.4s.cf.nc")), station_count, Convention.CF));
            gsodList.add(new GSODStructureNetCDFFile((new File(netcdfDirectory, "gsod.4s.uod.nc")), station_count, Convention.UOD));

            int count = gsodList.size();
            for (int index = 0; index < count ; ++index) {
                GSODNetCDFFileBase gsod = gsodList.get(index);
                for (int station_index = 0; station_index < station_count; ++station_index) {
                    ISHStation station = station_list.get(station_index);
                    out.println(gsod.name + " " + (index + 1) + " of " + count + ": processing " + (station_index +1) + " of " + station_count + " stations");
                    List<ISHStationObservation> observations = new ArrayList<ISHStationObservation>();
                    for (File dataFile : stationFileMap.get(station)) {
                        is = null;
                        try {
                            is = new BufferedInputStream(new FileInputStream(dataFile), 128 << 10);
                            List<ISHStationObservation> obs = ISHStationObservation.parse(is);
                            observations.addAll(obs);
                        } catch (Exception e) {
                            System.out.println("Error parsing " + dataFile.getName());
                        } finally {
                            if (is != null) { try { is.close(); is = null; } catch (IOException e) { } }
                        }
                    }
                    gsod.putObservations(station, observations);
                }

                gsod.close();
            }

        } catch (Exception ex) {
            Logger.getLogger(GSODNetCDFGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public enum Convention {
        CF,
        UOD;
    }
    
    public static abstract class GSODNetCDFFileBase {

        public final static long baseDateMilliseconds =
                new GregorianCalendar(1929, 0, 1).getTimeInMillis();

        public final static long millisecondsPerDay = 1000 * 60 * 60 * 24;

        public final String name;

        public final int createFlags;
        
        public final Convention convention;

        public final int ncId;

        public final int ncDimId_station;
        public final int ncDimId_station_id_len;

        public final int ncVarId_lon;
        public final int ncVarId_lat;
        public final int ncVarId_elev;
        public final int ncVarId_wmo;
        public final int ncVarId_wban;
        public final int ncVarId_station_id;
        public final int ncVarId_numChildren;
        public final int ncVarId_firstChild;

        public final int[] station_dimidsp;
        public final int[] station_id_dimidsp;

        GSODNetCDFFileBase(File file, int stationCount, int createFlags, Convention convention) {

            this.name = file.getName();
            
            this.createFlags = createFlags;
            
            this.convention = convention;

            int ncStatus;
            IntByReference iRef = new IntByReference();

            ncStatus = nc_create(file.getAbsolutePath(), createFlags, iRef); status(ncStatus);
            ncId = iRef.getValue();

            // DIMENSIONS
            ncStatus = nc_def_dim(ncId, STATION_DIM_NAME, new NativeLong(stationCount), iRef); status(ncStatus);
            ncDimId_station = iRef.getValue();
            ncStatus = nc_def_dim(ncId, "station_id_len", new NativeLong(128), iRef); status(ncStatus);
            ncDimId_station_id_len = iRef.getValue();

            //// VARIABLES
            // STATION
            station_dimidsp = new int[] { ncDimId_station };
            station_id_dimidsp = new int[] { ncDimId_station, ncDimId_station_id_len };
            ncStatus = nc_def_var(ncId, "lon", NC_FLOAT, station_dimidsp, iRef); status(ncStatus);
            ncVarId_lon = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_lon, "standard_name", "longitude"); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_lon, "units", "degrees_east"); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_lon, "_FillValue", -999.999f); status(ncStatus);

            ncStatus = nc_def_var(ncId, "lat", NC_FLOAT, station_dimidsp, iRef); status(ncStatus);
            ncVarId_lat = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_lat, "standard_name", "latitude"); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_lat, "units", "degrees_north"); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_lat, "_FillValue", -99.999f); status(ncStatus);

            ncStatus = nc_def_var(ncId, "elev", NC_FLOAT, station_dimidsp, iRef); status(ncStatus);
            ncVarId_elev = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_elev, "standard_name", "height"); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_elev, "units", "ft"); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_elev, "positive", "up"); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_elev, "_FillValue", -99.999f); status(ncStatus);

            ncStatus = nc_def_var(ncId, "wmo", NC_INT, station_dimidsp, iRef); status(ncStatus);
            ncVarId_wmo = iRef.getValue();
            nc_put_att_text(ncId, ncVarId_wmo, "standard_name", "station_WMO_id"); status(ncStatus);

            ncStatus = nc_def_var(ncId, "wban", NC_INT, station_dimidsp, iRef); status(ncStatus);
            ncVarId_wban = iRef.getValue();

            ncStatus = nc_def_var(ncId, "station_id", NC_CHAR, station_id_dimidsp , iRef); status(ncStatus);
            ncVarId_station_id = iRef.getValue();
            nc_put_att_text(ncId, ncVarId_station_id, "standard_name", "station_id"); status(ncStatus);
            if (convention == Convention.CF) {
                nc_put_att_text(ncId, ncVarId_station_id, "cf_role", "timeseries_id"); status(ncStatus);
            }
            
            ncStatus = nc_def_var(ncId, "numChildren", NC_INT, station_dimidsp , iRef); status(ncStatus);
            ncVarId_numChildren = iRef.getValue();
            if (convention == Convention.CF) {
                nc_put_att_text(ncId, ncVarId_numChildren, "CF:ragged_row_count", OBSERVATION_DIM_NAME); status(ncStatus);
                nc_put_att_text(ncId, ncVarId_numChildren, "sample_dimension", OBSERVATION_DIM_NAME); status(ncStatus);
                nc_put_att_text(ncId, ncVarId_numChildren, "standard_name", "ragged_row_size"); status(ncStatus);
            }
            
            ncStatus = nc_def_var(ncId, "firstChild", NC_INT, station_dimidsp , iRef); status(ncStatus);
            ncVarId_firstChild = iRef.getValue();
        }

        public abstract void putObservations(ISHStation station, List<ISHStationObservation> observations);

        public void close() {
            status(nc_close(ncId));
        }

        public void sync() {
            status(nc_sync(ncId));
        }

        private static void status(int status) {
            if(status != NC_NOERR) {
                out.println(nc_strerror(status));
            }
        }
    }

    public final static class GSODNetCDFFile extends GSODNetCDFFileBase {

        public final int ncDimId_observation;

        public final int ncVarId_time;
        public final int ncVarId_temp;
        public final int ncVarId_dewp;
        public final int ncVarId_slp;
        public final int ncVarId_stp;
        public final int ncVarId_visib;
        public final int ncVarId_wdsp;
        public final int ncVarId_mxspd;
        public final int ncVarId_gust;
        public final int ncVarId_max;
        public final int ncVarId_min;
        public final int ncVarId_prcp;
        public final int ncVarId_sndp;
        public final int ncVarId_frshtt;

        public final int[] observation_dimidsp;

        public GSODNetCDFFile(File file, int stationCount, int createFlags, Convention convention) {
            super(file, stationCount, createFlags, convention);

            int ncStatus;
            IntByReference iRef = new IntByReference();

            ncStatus = nc_def_dim(ncId, OBSERVATION_DIM_NAME, new NativeLong(0), iRef); status(ncStatus);
            ncDimId_observation = iRef.getValue();

            //// VARIABLES
            // OBSERVATION
            observation_dimidsp = new int[] { ncDimId_observation };
            String observation_coord = "time lon lat elev";
            ncStatus = nc_def_var(ncId, "time", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_time = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_time, "units", "days since 1929-01-01 00:00:00"); status(ncStatus);

            ncStatus = nc_def_var(ncId, "temp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_temp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_temp, "units", "degF"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_temp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_temp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_temp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_temp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "dewp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_dewp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_dewp, "units", "degF"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_dewp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_dewp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_dewp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_dewp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "slp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_slp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_slp, "units", "mbar"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_slp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_slp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_slp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_slp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "stp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_stp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_stp, "units", "mbar"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_stp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_stp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_stp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_stp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "visib", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_visib = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_visib, "units", "miles"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_visib, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_visib, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_visib, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_visib, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "wdsp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_wdsp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_wdsp, "units", "knots"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_wdsp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_wdsp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_wdsp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_wdsp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "mxspd", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_mxspd = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_mxspd, "units", "knots"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_mxspd, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_mxspd, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_mxspd, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_mxspd, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "gust", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_gust = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_gust, "units", "knots"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_gust, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_gust, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_gust, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_gust, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "max", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_max = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_max, "units", "degF"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_max, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_max, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_max, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_max, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "min", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_min = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_min, "units", "degF"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_min, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_min, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_min, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_min, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "prcp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_prcp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_prcp, "units", "inches"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_prcp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_prcp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_prcp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_prcp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "sndp", NC_SHORT, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_sndp = iRef.getValue();
            ncStatus = nc_put_att_text(ncId, ncVarId_sndp, "units", "inches"); status(ncStatus);
            ncStatus = nc_put_att_short(ncId, ncVarId_sndp, "_FillValue", _FillValue_SHORT); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_sndp, "scale_factor", 0.1f); status(ncStatus);
            ncStatus = nc_put_att_float(ncId, ncVarId_sndp, "add_offset", 0.0f); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_sndp, "coordinates", observation_coord); status(ncStatus);

            ncStatus = nc_def_var(ncId, "frshtt", NC_BYTE, observation_dimidsp, iRef); status(ncStatus);
            ncVarId_frshtt = iRef.getValue();
            ncStatus = nc_put_att_ubyte(ncId, ncVarId_frshtt, "_FillValue", (byte) 0); status(ncStatus);
            ncStatus = nc_put_att_text(ncId, ncVarId_frshtt, "coordinates", observation_coord); status(ncStatus);
            
            // GLOBAL ATTRIBUTES
            if (convention == Convention.CF) {
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "Conventions", "CF-1.6"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "CF:featureType", "timeSeries"); status(ncStatus);
            } else {
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "Conventions", "Unidata Observation Dataset v1.0"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "cdm_datatype", "Station"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "stationDimension", STATION_DIM_NAME); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "observationDimension", OBSERVATION_DIM_NAME); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "cdm_datatype", "STATION"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "latitude_coordinate", "lat"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "longitude_coordinate", "lon"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "zaxis_coordinate", "elev"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "time_coordinate", "time"); status(ncStatus);
            }

            ncStatus = nc_enddef(ncId); status(ncStatus);
        }

        private final NativeLong station_indexp = new NativeLong(0);
        private final NativeLong stationid_len_indexp = new NativeLong(0);
        private final Calendar observationDate = new GregorianCalendar();

        private final NativeLongByReference observation_startp = new NativeLongByReference(new NativeLong(0));
        private final NativeLongByReference observation_countp = new NativeLongByReference(new NativeLong(0));

        @Override
        public void putObservations(ISHStation station, List<ISHStationObservation> observations) {
            int ncStatus = 0;
            ncStatus = nc_put_var1_float(ncId, ncVarId_lon, station.longitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_float(ncId, ncVarId_lat, station.latitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_float(ncId, ncVarId_elev, station.elevation, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_wmo, Integer.parseInt(station.wmo), station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_wban, Integer.parseInt(station.wban), station_indexp); status(ncStatus);
            ncStatus = nc_put_vara_text(ncId, ncVarId_station_id, station.filePrefix, station_indexp, stationid_len_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_numChildren, observations.size(), station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_firstChild, observation_startp.getValue().intValue(), station_indexp); status(ncStatus);
            
            ShortBuffer timeBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer tempBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer dewpBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer slpBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer stpBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer visibBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer wdspBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer mxspdBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer gustBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer maxBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer minBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer prcpBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ShortBuffer sndpBuffer = ByteBuffer.allocateDirect(observations.size() * 2).order(ByteOrder.nativeOrder()).asShortBuffer();
            ByteBuffer frshttBuffer = ByteBuffer.allocateDirect(observations.size());

            for (ISHStationObservation observation : observations) {
                observationDate.clear();
                observationDate.set(observation.year, observation.month - 1, observation.day);
                long dateDeltaMilis = observationDate.getTimeInMillis() - baseDateMilliseconds;
                short dateDeltaDays = (short) (dateDeltaMilis / millisecondsPerDay);

                timeBuffer.put(dateDeltaDays);
                tempBuffer.put(packValue(observation.temp, 9999.9f, 10f));
                dewpBuffer.put(packValue(observation.dewp, 9999.9f, 10f));
                slpBuffer.put(packValue(observation.slp, 9999.9f, 10f));
                stpBuffer.put(packValue(observation.stp, 9999.9f, 10f));
                visibBuffer.put(packValue(observation.visib, 999.9f, 10f));
                wdspBuffer.put(packValue(observation.wdsp, 999.9f, 10f));
                mxspdBuffer.put(packValue(observation.mxspd, 999.9f, 10f));
                gustBuffer.put(packValue(observation.gust, 999.9f, 10f));
                maxBuffer.put(packValue(observation.max, 9999.9f, 10f));
                minBuffer.put(packValue(observation.min, 9999.9f, 10f));
                prcpBuffer.put(packValue(observation.prcp, 99.99f, 100f));
                sndpBuffer.put(packValue(observation.sndp, 999.9f, 10f));
                frshttBuffer.put((byte)observation.frshtt);
            }

            observation_countp.setValue(new NativeLong(observations.size()));

            ncStatus = nc_put_vara(ncId, ncVarId_time, observation_startp, observation_countp, timeBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_temp, observation_startp, observation_countp, tempBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_dewp, observation_startp, observation_countp, dewpBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_slp, observation_startp, observation_countp, slpBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_stp, observation_startp, observation_countp, stpBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_visib, observation_startp, observation_countp, visibBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_wdsp, observation_startp, observation_countp, wdspBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_mxspd, observation_startp, observation_countp, mxspdBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_gust, observation_startp, observation_countp, gustBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_max, observation_startp, observation_countp, maxBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_min, observation_startp, observation_countp, minBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_prcp, observation_startp, observation_countp, prcpBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_sndp, observation_startp, observation_countp, sndpBuffer); status(ncStatus);
            ncStatus = nc_put_vara(ncId, ncVarId_frshtt, observation_startp, observation_countp, frshttBuffer); status(ncStatus);

            observation_startp.setValue(new NativeLong(observation_startp.getValue().intValue() + observations.size()));

            station_indexp.setValue(station_indexp.longValue() + 1);
        }


    }

    public final static class GSODStructureNetCDFFile extends GSODNetCDFFileBase {


        public final int ncDimId_record;

        public final int ncTypeId_record_type;

        public final int ncVarId_record;

        public final int[] record_dimidsp;

        public GSODStructureNetCDFFile(File file, int stationCount, Convention convention ) {
            super(file, stationCount, NC_NETCDF4, convention);

            int ncStatus;
            IntByReference iRef = new IntByReference();


            ncStatus = nc_def_dim(ncId, OBSERVATION_DIM_NAME, new NativeLong(0), iRef); status(ncStatus);
            ncDimId_record = iRef.getValue();

            //// VARIABLES
            // OBSERVATION
            ncStatus = nc_def_compound(ncId, new NativeLong(27), OBSERVATION_STRUCT_NAME + "_type", iRef); status(ncStatus);
            ncTypeId_record_type = iRef.getValue();
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "time"  , new NativeLong(0) , NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "temp"  , new NativeLong(2) , NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "dewp"  , new NativeLong(4) , NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "slp"   , new NativeLong(6) , NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "stp"   , new NativeLong(8) , NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "visib" , new NativeLong(10), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "wdsp"  , new NativeLong(12), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "mxspd" , new NativeLong(14), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "gust"  , new NativeLong(16), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "max"   , new NativeLong(18), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "min"   , new NativeLong(20), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "prcp"  , new NativeLong(22), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "sndp"  , new NativeLong(24), NC_SHORT); status(ncStatus);
            ncStatus = nc_insert_compound(ncId, ncTypeId_record_type, "frshtt", new NativeLong(26), NC_UBYTE); status(ncStatus);

            record_dimidsp = new int[] { ncDimId_record };
            ncStatus = nc_def_var(ncId, OBSERVATION_STRUCT_NAME, ncTypeId_record_type, record_dimidsp , iRef); status(ncStatus);
            ncVarId_record = iRef.getValue();

            ncStatus = nc_put_att_text(ncId, ncVarId_record, "coordinates", OBSERVATION_STRUCT_NAME + ".time lon lat elev");

            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "_units_type", "units",
                    new String[] {"time","temp","dewp","slp","stp","visib","wdsp","mxspd","gust","max","min","prcp","sndp"},
                    new String[] {"days since 1929-01-01 00:00:00","degF","degF","mbar","mbar","miles","knots","knots","knots","degF","degF","inches","inches"}
            );

            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "_scale_factor_type", "scale_factor",
                    new String[] {"temp","dewp","slp","stp","visib","wdsp","mxspd","gust","max","min","prcp","sndp"},
                    new float[] {0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.01f, 0.1f}
            );
            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "_add_offset_type", "add_offset",
                    new String[] {"temp","dewp","slp","stp","visib","wdsp","mxspd","gust","max","min","prcp","sndp"},
                    new float[] {0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}
            );
//            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "__FillValue_type", _FillValue,
//                    new String[] {"temp","dewp","slp","stp","visib","wdsp","mxspd","gust","max","min","prcp","sndp"},
//                    new short[] {_FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT}
//            );
            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "_missing_value_type", "missing_value",
                    new String[] {"temp","dewp","slp","stp","visib","wdsp","mxspd","gust","max","min","prcp","sndp"},
                    new short[] {_FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT, _FillValue_SHORT}
            );
            
            generateCompoundAttributes(ncId, ncVarId_record, OBSERVATION_STRUCT_NAME + "_standard_name_type", "standard_name",
                    new String[] {"time" },
                    new String[] {"time" }
            );

            ncStatus = nc_def_var_chunking(ncId, ncVarId_record, NC_CHUNKED, new NativeLong[] { new NativeLong(27) }); status(ncStatus);

            // GLOBAL ATTRIBUTES
            if (convention == Convention.CF) {
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "Conventions", "CF-1.6"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "CF:featureType", "timeSeries"); status(ncStatus);
            } else {
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "Conventions", "Unidata Observation Dataset v1.0"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "cdm_datatype", "Station"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "stationDimension", STATION_DIM_NAME); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "observationDimension", OBSERVATION_DIM_NAME); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "cdm_datatype", "STATION"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "latitude_coordinate", "lat"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "longitude_coordinate", "lon"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "zaxis_coordinate", "elev"); status(ncStatus);
                ncStatus = nc_put_att_text(ncId, NC_GLOBAL, "time_coordinate", OBSERVATION_STRUCT_NAME + ".time"); status(ncStatus);
            }

            ncStatus = nc_enddef(ncId); status(ncStatus);

        }

        private final NativeLong station_indexp = new NativeLong(0);
        private final NativeLong station_id_len_indexp = new NativeLong(0);
        private final Calendar recordDate = new GregorianCalendar();
        private final NativeLongByReference record_startp = new NativeLongByReference(new NativeLong(0));
        private final NativeLongByReference record_countp = new NativeLongByReference(new NativeLong(0));

        @Override
        public void putObservations(ISHStation station, List<ISHStationObservation> observations) {
            int ncStatus = 0;

            ncStatus = nc_put_var1_float(ncId, ncVarId_lon, station.longitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_float(ncId, ncVarId_lat, station.latitude, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_float(ncId, ncVarId_elev, station.elevation, station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_wmo, Integer.parseInt(station.wmo), station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_wban, Integer.parseInt(station.wban), station_indexp); status(ncStatus);
            ncStatus = nc_put_vara_text(ncId, ncVarId_station_id, station.filePrefix, station_indexp, station_id_len_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_numChildren, observations.size(), station_indexp); status(ncStatus);
            ncStatus = nc_put_var1_int(ncId, ncVarId_firstChild, record_startp.getValue().intValue(), station_indexp); status(ncStatus);

            ByteBuffer recordBuffer = ByteBuffer.allocateDirect(27 * observations.size());
            recordBuffer.order(ByteOrder.nativeOrder());
            for (ISHStationObservation observation : observations) {
                recordDate.clear();
                recordDate.set(observation.year, observation.month - 1, observation.day);
                long dateDeltaMillis = recordDate.getTimeInMillis() - baseDateMilliseconds;
                short value = (short) (dateDeltaMillis / millisecondsPerDay);

                recordBuffer.putShort(value);
                recordBuffer.putShort(packValue(observation.temp, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.dewp, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.slp, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.stp, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.visib, 999.9f, 10f));
                recordBuffer.putShort(packValue(observation.wdsp, 999.9f, 10f));
                recordBuffer.putShort(packValue(observation.mxspd, 999.9f, 10f));
                recordBuffer.putShort(packValue(observation.gust, 999.9f, 10f));
                recordBuffer.putShort(packValue(observation.max, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.min, 9999.9f, 10f));
                recordBuffer.putShort(packValue(observation.prcp, 99.99f, 100f));
                recordBuffer.putShort(packValue(observation.sndp, 999.9f, 10f));
                recordBuffer.put((byte)(observation.frshtt));
            }
            recordBuffer.rewind();

            record_countp.setValue(new NativeLong(observations.size()));

            ncStatus = nc_put_vara(ncId, ncVarId_record, record_startp, record_countp, recordBuffer);
            
            record_startp.setValue(new NativeLong(record_startp.getValue().intValue() + observations.size()));

            station_indexp.setValue(station_indexp.longValue() + 1);
        }

    }

    private static short packValue(float value, float missing, float scale) {
        float delta = value - missing;
        if (delta < 0) delta = -delta;
        return delta < 1e-5 ? _FillValue_SHORT : (short) Math.round(value * scale);
    }
}
