package gov.usgs.cida.netcdfgsod;

public class ISHStation {

    public final String wmo;
    public final String wban;
    public final String name;
    public final String fips;
    public final String state;
    public final String call;
    public final float latitude;
    public final float longitude;
    public final float elevation;
    public final String filePrefix;

    public ISHStation(String wmo, String wban, String name, String fips, String state, String call, String latitude, String longitude, String elevation) {
        this.wmo = wmo;
        this.wban = wban;
        this.name = name;
        this.fips = fips;
        this.state = state;
        this.call = call;
        if (latitude.startsWith("+")) {
            latitude = latitude.substring(1);
        }
        if (longitude.startsWith("+")) {
            longitude = longitude.substring(1);
        }
        if (elevation.startsWith("+")) {
            elevation = elevation.substring(1);
        }
        this.latitude = (float) ((double) Integer.parseInt(latitude) / 1000.0);
        this.longitude = (float) ((double) Integer.parseInt(longitude) / 1000.0);
        this.elevation = (float) ((double) Integer.parseInt(elevation) / 10.0);

        filePrefix = wmo + "-" + wban;
    }

}
