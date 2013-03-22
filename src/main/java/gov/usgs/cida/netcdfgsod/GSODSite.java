/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.netcdfgsod;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.String;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;


/**
 *
 * @author tkunicki
 */
public class GSODSite {

    
    

    private static GSODSite instance;

    public static synchronized GSODSite getInstance() {
        if (instance == null) {
            instance = new GSODSite();
        }
        return instance;
    }

    private FTPClient ftpClient;

    private ISHHistory ishHistory;
    private List<String> availableYears;

    private GSODSite() {
    }

    private void verifyConnection() throws IOException {
        if (ftpClient == null) {
            ftpClient = new FTPClient();
        }
        if (!ftpClient.isConnected()) {
            ftpClient.connect(GSODConstants.HOSTNAME);
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                throw new IOException("FTP Connection Error: (" + replyCode + ") " + ftpClient.getReplyString());
            }
            if(!ftpClient.login("ftp", "ftp")) {
                throw new IOException("FTP Login Error: (" + replyCode + ") " + ftpClient.getReplyString());
            }
        }
    }
    private void changeToGSODRootDirectory() throws IOException {
        ftpClient.changeWorkingDirectory(GSODConstants.PATH_ROOT);
    }

    private void changeToDirectory(String path) throws IOException {
        ftpClient.changeWorkingDirectory(path);
    }

    public synchronized ISHHistory getISHHistory() throws IOException {
        if (ishHistory == null) {
            verifyConnection();
            changeToGSODRootDirectory();
            InputStream inputStream = ftpClient.retrieveFileStream(GSODConstants.FILE_ISH_CSV);
            if (inputStream != null) {
                ishHistory = new ISHHistory();
                try {
                    ishHistory.parse(inputStream);
                } catch (IOException e) {
                   e.printStackTrace();
                }finally {
                    ftpClient.completePendingCommand();
                }
            }
        }
        return ishHistory;
    }

    public void shutdown() throws IOException {
        if(ftpClient.isConnected()) {
            ftpClient.disconnect();
        }
    }

    public synchronized List<String> getAvailableYears() throws IOException {
        if (availableYears == null) {
            availableYears = new ArrayList<String>();
//            verifyConnection();
//            FTPFile[] ftpFiles = ftpClient.listFiles(GSODConstants.PATH_ROOT);
//            if(ftpFiles != null) {
//                for (FTPFile ftpFile : ftpFiles) {
//                    if (ftpFile.isDirectory() ) {
//                        String name = ftpFile.getName();
//                        if (GSODConstants.PATTERN_YEAR.matcher(name).matches() ) {
//                            availableYears.add(name);
//                        }
//                    }
//                }
//            }
//            Collections.sort(availableYears);
			availableYears.add("2010");
            availableYears.add("2011");
        }
        return availableYears;
    }

    public void pullData(String downloadPath) throws IOException {
        if(downloadPath == null) {
            throw new NullPointerException("downloadPath is null");
        }

        File downloadDirectory = new File(downloadPath);
        if(!downloadDirectory.exists()) {
            boolean success = downloadDirectory.mkdirs();
            if (!success) {
                throw new IOException("unable to create " + downloadPath);
            }
        }

        long bytes = 0;
        verifyConnection();
        for(String year : getAvailableYears()) {
            System.out.println("looking at files for year " + year + "...");
            int filesDownloaded = 0;
            int filesSkipped = 0;
            int filesCurrent = 0;
            int filesTimeStamped = 0;

            ftpClient.changeWorkingDirectory(GSODConstants.PATH_ROOT + year);

            File currentDirectory = new File(downloadDirectory, year);
            if( !currentDirectory.exists()) {
                currentDirectory.mkdir();
            }

            FTPFile[] ftpFiles = ftpClient.listFiles();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            for (FTPFile ftpFile : ftpFiles) {
                Matcher matcher = GSODConstants.PATTERN_prefix.matcher(ftpFile.getName());
                if(matcher.find()) {
                    String prefix = matcher.group(1);
                    if(getISHHistory().getStation(prefix) != null) {
                        File localFile = new File(currentDirectory, ftpFile.getName());
                        long remoteModified = ftpFile.getTimestamp().getTimeInMillis();
                        if (!localFile.exists() || localFile.length() != ftpFile.getSize()) {
                            bytes += ftpFile.getSize();
                            filesDownloaded++;
                            System.out.print("downloading   " + ftpFile.getName() + " (" + (ftpFile.getSize() >> 10) + " KiBi)...");
                            OutputStream os = new FileOutputStream(localFile);
                            boolean success = ftpClient.retrieveFile(GSODConstants.PATH_ROOT + year + "/" + ftpFile.getName(), os);
                            os.close();
                            if(!success) {
                                System.out.println(ftpClient.getReplyCode() + " " + ftpClient.getReplyString());
                                shutdown();
                                System.exit(0);
                            } else {
                                 System.out.println("Success!");
                                 localFile.setLastModified(remoteModified);
                            }
//                        } else {
                            if (localFile.lastModified() != remoteModified) {
                                localFile.setLastModified(remoteModified);
                                filesTimeStamped++;
                                System.out.println("timestamped " + ftpFile.getName() + " (" + (ftpFile.getSize() >> 10) + " KiBi)...");
                            } else {
                                filesCurrent++;
                                System.out.println("up to date  " + ftpFile.getName() + " (" + (ftpFile.getSize() >> 10) + " KiBi)...");
                            }
                        }
                    } else {
                        filesSkipped++;
                    }
                }
            }
            System.out.println(year + " Done.");
            System.out.println("  Downloaded  " + filesDownloaded);
            System.out.println("  Current     " + filesCurrent);
            System.out.println("  Timestamped " + filesTimeStamped);
            System.out.println("  Skipped     " + filesSkipped);
        }
        System.out.println("pulled down " + bytes + " (" + (bytes >> 20) + " MiBi).");

    }

    public static void localStationInventory(String downloadPath) throws IOException {
        File root = new File(downloadPath);
        File inventory = new File(root, GSODConstants.FILE_ISH_CSV);
        ISHHistory history = new ISHHistory();
        history.parse(new FileInputStream(inventory));
        File fileList[] = root.listFiles();
        List<File> yearList = new ArrayList<File>();
        for (File file : fileList) {
            if(GSODConstants.PATTERN_YEAR.matcher(file.getName()).matches()) {
                yearList.add(file);
            }
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(root, "matrix.csv")));
        writer.append("wmo,wban,lat,long,state,call");
        for (File year : yearList) {
            writer.append(',').append(year.getName()).append(',').append("first").append(',').append("last");
        }
        writer.newLine();
        StringBuilder first = new StringBuilder();
        StringBuilder last = new StringBuilder();
        for (ISHStation station : history.getStationList()) {
            writer.append(station.wmo);
            writer.append(',').append(station.wban);
            writer.append(',').append(Float.toString(station.latitude));
            writer.append(',').append(Float.toString(station.longitude));
            writer.append(',').append(station.state);
            writer.append(',').append(station.call);
            System.out.print("parsing " + station.wmo + "-" + station.wban);
            for (File year : yearList) {
                int count = 0;
                first.setLength(0);
                last.setLength(0);
                File observationFile = new File(year, station.wmo + "-" + station.wban + "-" + year.getName() + ".op.gz");
                if (observationFile.exists()) {
                    System.out.print(" " + year.getName());
                    InputStream is = null;
                    try {
                        is = new GZIPInputStream(new BufferedInputStream(new FileInputStream(observationFile)));
                        List<ISHStationObservation> obs = ISHStationObservation.parse(is);
                        count = obs.size();
                        if (count > 0) {
                           ISHStationObservation obsFirst = obs.get(0);
                           ISHStationObservation obsLast = obs.get(obs.size() - 1);
                           first.append(obsFirst.month).append('/').append(obsFirst.day).append('/').append(obsFirst.year);
                           last.append(obsLast.month).append('/').append(obsLast.day).append('/').append(obsLast.year);;
                        }
                    } catch (IOException e){
                        count = -1;
                    } finally {
                        try {
                            if (is != null) is.close();
                        } catch (IOException e) { }
                    }
                }
                writer.append(',').append(Integer.toString(count)).append(',').append(first.toString()).append(',').append(last.toString());
            }
            System.out.println(" done.");
            System.out.flush();
            writer.newLine();
        }
        writer.close();
    }

    public static void main(String[] args) {
        String downloadPath = "/Users/tkunicki/Data/GSOD";
        try {
            GSODSite.getInstance().pullData(downloadPath);
//            GSODSite.getInstance().getISHHistory();
//            GSODSite.localStationInventory(downloadPath);
        }
        catch (IOException ex) {
            Logger.getLogger(GSODSite.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                GSODSite.getInstance().shutdown();
            } catch (IOException ex) {
                Logger.getLogger(GSODSite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
